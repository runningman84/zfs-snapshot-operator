package operator

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/runningman84/zfs-snapshot-operator/pkg/config"
	"github.com/runningman84/zfs-snapshot-operator/pkg/models"
	"github.com/runningman84/zfs-snapshot-operator/pkg/zfs"
)

// Operator manages ZFS snapshot operations
type Operator struct {
	config        *config.Config
	manager       *zfs.Manager
	deletionCount int // Track number of deletions in current run
}

// NewOperator creates a new operator instance
func NewOperator(cfg *config.Config) *Operator {
	return &Operator{
		config:  cfg,
		manager: zfs.NewManager(cfg),
	}
}

// Run executes the snapshot management logic
func (o *Operator) Run() error {
	// Acquire lock to prevent concurrent runs
	if err := o.acquireLock(); err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}
	defer o.releaseLock()

	// Reset deletion counter
	o.deletionCount = 0

	now := time.Now()

	o.logConfig(now)

	// Get and log ZFS version information
	userland, kernel, err := o.manager.GetVersion()
	if err != nil {
		return fmt.Errorf("failed to get ZFS version: %w", err)
	}
	log.Printf("ZFS Version - Userland: %s, Kernel: %s", userland, kernel)

	// Get pool health status first
	poolStatus, err := o.manager.GetPoolStatus()
	if err != nil {
		return fmt.Errorf("failed to get pool status: %w", err)
	}

	pools, err := o.manager.GetPools()
	if err != nil {
		return fmt.Errorf("failed to get pools: %w", err)
	}

	// Track errors during processing
	var errors []error
	for _, pool := range pools {
		if err := o.processPool(pool, now, poolStatus); err != nil {
			log.Printf("Error processing pool %s: %v", pool.PoolName, err)
			errors = append(errors, fmt.Errorf("pool %s: %w", pool.PoolName, err))
		}
	}

	// Return error if any pools had issues
	if len(errors) > 0 {
		return fmt.Errorf("operator encountered %d error(s) during execution", len(errors))
	}

	log.Printf("Run completed successfully - created snapshots, deleted %d snapshot(s)", o.deletionCount)
	return nil
}

// acquireLock creates a lock file to prevent concurrent runs
func (o *Operator) acquireLock() error {
	lockPath := o.config.LockFilePath

	// Check if lock file exists
	if _, err := os.Stat(lockPath); err == nil {
		return fmt.Errorf("lock file exists at %s - another instance may be running", lockPath)
	}

	// Create lock file
	file, err := os.Create(lockPath)
	if err != nil {
		return fmt.Errorf("failed to create lock file: %w", err)
	}
	defer file.Close()

	// Write PID to lock file
	pid := os.Getpid()
	if _, err := file.WriteString(fmt.Sprintf("%d\n", pid)); err != nil {
		return fmt.Errorf("failed to write PID to lock file: %w", err)
	}

	log.Printf("Acquired lock (PID %d) at %s", pid, lockPath)
	return nil
}

// releaseLock removes the lock file
func (o *Operator) releaseLock() {
	lockPath := o.config.LockFilePath
	if err := os.Remove(lockPath); err != nil {
		log.Printf("Warning: failed to remove lock file %s: %v", lockPath, err)
	} else {
		log.Printf("Released lock at %s", lockPath)
	}
}

func (o *Operator) logConfig(now time.Time) {
	log.Println("Current config")
	log.Printf("Log level: %s", o.config.LogLevel)
	log.Printf("Max hourly snapshots: %d", o.config.MaxHourlySnapshots)
	log.Printf("Max daily snapshots: %d", o.config.MaxDailySnapshots)
	log.Printf("Max weekly snapshots: %d", o.config.MaxWeeklySnapshots)
	log.Printf("Max monthly snapshots: %d", o.config.MaxMonthlySnapshots)
	log.Printf("Max yearly snapshots: %d", o.config.MaxYearlySnapshots)
	if len(o.config.PoolWhitelist) > 0 {
		log.Printf("Pool whitelist: %v", o.config.PoolWhitelist)
	} else {
		log.Printf("Pool whitelist: all pools")
	}
	if len(o.config.FilesystemWhitelist) > 0 {
		log.Printf("Filesystem whitelist: %v", o.config.FilesystemWhitelist)
	} else {
		log.Printf("Filesystem whitelist: all filesystems")
	}
	log.Printf("Max hourly snapshot age: %s", o.config.GetMaxSnapshotDate("hourly", now).Format("2006-01-02 15:04:05"))
	log.Printf("Max daily snapshot age: %s", o.config.GetMaxSnapshotDate("daily", now).Format("2006-01-02 15:04:05"))
	log.Printf("Max weekly snapshot age: %s", o.config.GetMaxSnapshotDate("weekly", now).Format("2006-01-02 15:04:05"))
	log.Printf("Max monthly snapshot age: %s", o.config.GetMaxSnapshotDate("monthly", now).Format("2006-01-02 15:04:05"))
	log.Printf("Max yearly snapshot age: %s", o.config.GetMaxSnapshotDate("yearly", now).Format("2006-01-02 15:04:05"))
	log.Printf("Min hourly snapshot age: %s", o.config.GetMinSnapshotDate("hourly", now).Format("2006-01-02 15:04:05"))
	log.Printf("Min daily snapshot age: %s", o.config.GetMinSnapshotDate("daily", now).Format("2006-01-02 15:04:05"))
	log.Printf("Min weekly snapshot age: %s", o.config.GetMinSnapshotDate("weekly", now).Format("2006-01-02 15:04:05"))
	log.Printf("Min monthly snapshot age: %s", o.config.GetMinSnapshotDate("monthly", now).Format("2006-01-02 15:04:05"))
	log.Printf("Min yearly snapshot age: %s", o.config.GetMinSnapshotDate("yearly", now).Format("2006-01-02 15:04:05"))
}

func (o *Operator) processPool(pool *models.Pool, now time.Time, poolStatus map[string]*models.PoolStatus) error {
	// Check if pool is in whitelist
	if !o.config.IsPoolAllowed(pool.PoolName) {
		log.Printf("Skipping pool %s (not in whitelist)", pool.PoolName)
		return nil
	}

	// Check pool health before any operations (only log once per unique pool)
	if !o.manager.IsPoolHealthy(pool.PoolName, poolStatus) {
		log.Printf("Skipping pool %s due to health issues", pool.PoolName)
		return fmt.Errorf("pool %s is not healthy", pool.PoolName)
	}

	if pool.FilesystemName == "" {
		// This is a pool root without a specific filesystem
		log.Printf("Processing pool %s (root)", pool.PoolName)

		// Log pool usage and check for errors
		o.logPoolStatus(pool.PoolName, poolStatus)

		// Check if scrub is older than 3 months
		o.checkScrubAge(pool.PoolName, poolStatus, now)

		log.Printf("Ignoring pool root without filesystem %s", pool.PoolName)
		return nil
	}

	// Check if filesystem is in whitelist
	if !o.config.IsFilesystemAllowed(pool.FilesystemName) {
		log.Printf("Skipping filesystem %s (not in whitelist)", pool.FilesystemName)
		return nil
	}

	log.Printf("Processing filesystem %s", pool.FilesystemName)

	// Log filesystem usage
	o.logFilesystemUsage(pool)

	for _, frequency := range config.Frequencies() {
		if err := o.processFrequency(pool, frequency, now); err != nil {
			log.Printf("Error processing frequency %s: %v", frequency, err)
		}
	}

	// Log snapshot summary for this filesystem
	o.logSnapshotSummary(pool)

	log.Printf("Finished filesystem %s", pool.FilesystemName)
	log.Printf("Finished pool %s", pool.PoolName)

	return nil
}

func (o *Operator) logPoolStatus(poolName string, poolStatus map[string]*models.PoolStatus) {
	status, exists := poolStatus[poolName]
	if !exists {
		return
	}

	// Check for errors
	hasErrors := false
	if status.ReadErrors != "" && status.ReadErrors != "0" {
		log.Printf("WARNING: Pool %s has %s read error(s)", poolName, status.ReadErrors)
		hasErrors = true
	}
	if status.WriteErrors != "" && status.WriteErrors != "0" {
		log.Printf("WARNING: Pool %s has %s write error(s)", poolName, status.WriteErrors)
		hasErrors = true
	}
	if status.ChecksumErrors != "" && status.ChecksumErrors != "0" {
		log.Printf("WARNING: Pool %s has %s checksum error(s)", poolName, status.ChecksumErrors)
		hasErrors = true
	}

	if hasErrors {
		log.Printf("WARNING: Pool %s has errors - consider running 'zpool scrub %s'", poolName, poolName)
	}
}

func (o *Operator) logFilesystemUsage(pool *models.Pool) {
	if pool.Used == "" || pool.Avail == "" {
		return
	}

	// Calculate percentage
	used := parseSize(pool.Used)
	avail := parseSize(pool.Avail)
	if used > 0 && avail > 0 {
		total := used + avail
		percent := float64(used) / float64(total) * 100
		log.Printf("Filesystem %s usage: %s used, %s available (%.1f%%)", pool.FilesystemName, pool.Used, pool.Avail, percent)
	} else {
		log.Printf("Filesystem %s usage: %s used, %s available", pool.FilesystemName, pool.Used, pool.Avail)
	}
}

func (o *Operator) checkScrubAge(poolName string, poolStatus map[string]*models.PoolStatus, now time.Time) {
	status, exists := poolStatus[poolName]
	if !exists {
		return
	}

	// If no scrub information available, warn
	if status.ScrubState == "none" || status.LastScrubTime == 0 {
		log.Printf("WARNING: Pool %s has no scrub information - consider running 'zpool scrub %s'", poolName, poolName)
		return
	}

	// Calculate age of last scrub
	lastScrub := time.Unix(status.LastScrubTime, 0)
	age := now.Sub(lastScrub)
	threshold := time.Duration(o.config.ScrubAgeThresholdDays) * 24 * time.Hour

	if age > threshold {
		days := int(age.Hours() / 24)
		log.Printf("WARNING: Pool %s last scrub was %d days ago (last scrub: %s) - consider running 'zpool scrub %s'",
			poolName, days, lastScrub.Format("2006-01-02 15:04:05"), poolName)
	} else if status.ScrubState == "in_progress" {
		log.Printf("Pool %s scrub is currently in progress (started: %s)", poolName, lastScrub.Format("2006-01-02 15:04:05"))
	} else {
		// Scrub is recent and finished - log the info
		days := int(age.Hours() / 24)
		if days == 0 {
			hours := int(age.Hours())
			log.Printf("Pool %s last scrub completed %d hour(s) ago (finished: %s)", poolName, hours, lastScrub.Format("2006-01-02 15:04:05"))
		} else {
			log.Printf("Pool %s last scrub completed %d day(s) ago (finished: %s)", poolName, days, lastScrub.Format("2006-01-02 15:04:05"))
		}
	}
}

func (o *Operator) processFrequency(pool *models.Pool, frequency string, now time.Time) error {
	log.Printf("Processing frequency %s", frequency)

	snapshots, err := o.manager.GetSnapshots(pool.PoolName, pool.FilesystemName, frequency)
	if err != nil {
		return fmt.Errorf("failed to get snapshots: %w", err)
	}

	// Count how many snapshots will remain after deletion
	var snapshotsToDelete []*models.Snapshot
	var snapshotsToKeep []*models.Snapshot

	for _, snapshot := range snapshots {
		if o.manager.CanSnapshotBeDeleted(snapshot, frequency, now) {
			snapshotsToDelete = append(snapshotsToDelete, snapshot)
		} else {
			snapshotsToKeep = append(snapshotsToKeep, snapshot)
		}
	}

	// Safety check: Never delete all snapshots - always keep at least 1
	if len(snapshotsToKeep) == 0 && len(snapshotsToDelete) > 0 {
		log.Printf("WARNING: Refusing to delete all %d snapshots for %s - keeping newest snapshot as safety measure",
			len(snapshotsToDelete), frequency)
		// Keep the newest snapshot
		if len(snapshotsToDelete) > 0 {
			newest := snapshotsToDelete[0]
			for _, s := range snapshotsToDelete {
				if s.DateTime.After(newest.DateTime) {
					newest = s
				}
			}
			snapshotsToKeep = append(snapshotsToKeep, newest)
			// Remove from delete list
			var filtered []*models.Snapshot
			for _, s := range snapshotsToDelete {
				if s.SnapshotName != newest.SnapshotName {
					filtered = append(filtered, s)
				}
			}
			snapshotsToDelete = filtered
		}
	}

	// Process deletions with limit check
	for _, snapshot := range snapshotsToDelete {
		// Check deletion limit
		if o.deletionCount >= o.config.MaxDeletionsPerRun {
			log.Printf("WARNING: Reached deletion limit of %d snapshots - skipping remaining deletions", o.config.MaxDeletionsPerRun)
			break
		}

		if o.config.DryRun {
			log.Printf("[DRY-RUN] Would delete snapshot %s", snapshot.SnapshotName)
			o.deletionCount++
		} else {
			if err := o.manager.DeleteSnapshot(snapshot); err != nil {
				log.Printf("Failed to delete snapshot: %v", err)
			} else {
				o.deletionCount++
			}
		}
	}

	// Log kept snapshots
	for _, snapshot := range snapshotsToKeep {
		log.Printf("Keeping snapshot %s", snapshot.SnapshotName)
	}

	// Find recent snapshot
	var snapshotRecent *models.Snapshot
	for _, snapshot := range snapshots {
		if o.manager.IsSnapshotRecent(snapshot, frequency, now) {
			if snapshotRecent == nil || snapshotRecent.DateTime.Before(snapshot.DateTime) {
				snapshotRecent = snapshot
			}
		}
	}

	if snapshotRecent != nil {
		log.Printf("Found recent snapshot %s", snapshotRecent.SnapshotName)
	} else {
		log.Printf("Did not find any recent snapshot for frequency %s", frequency)

		formattedTime := now.Format("2006-01-02_15:04:05")
		snapshotName := fmt.Sprintf("autosnap_%s_%s", formattedTime, frequency)

		newSnapshot := &models.Snapshot{
			PoolName:       pool.PoolName,
			FilesystemName: pool.FilesystemName,
			SnapshotName:   snapshotName,
			DateTime:       now,
			Frequency:      frequency,
		}

		if o.config.DryRun {
			log.Printf("[DRY-RUN] Would create snapshot %s", snapshotName)
		} else {
			if err := o.manager.CreateSnapshot(newSnapshot); err != nil {
				return fmt.Errorf("failed to create snapshot: %w", err)
			}
		}
	}

	return nil
}

func (o *Operator) logSnapshotSummary(pool *models.Pool) {
	log.Printf("Snapshot summary for %s:", pool.FilesystemName)

	for _, frequency := range config.Frequencies() {
		snapshots, err := o.manager.GetSnapshots(pool.PoolName, pool.FilesystemName, frequency)
		if err != nil {
			log.Printf("  Error getting %s snapshots: %v", frequency, err)
			continue
		}

		if len(snapshots) == 0 {
			log.Printf("  %s: %d snapshot(s)", frequency, len(snapshots))
			continue
		}

		// Find oldest and newest snapshots
		var oldest, newest *models.Snapshot
		for _, snapshot := range snapshots {
			if oldest == nil || snapshot.DateTime.Before(oldest.DateTime) {
				oldest = snapshot
			}
			if newest == nil || snapshot.DateTime.After(newest.DateTime) {
				newest = snapshot
			}
		}

		log.Printf("  %s: %d snapshot(s) [oldest: %s, newest: %s]",
			frequency, len(snapshots),
			oldest.DateTime.Format("2006-01-02 15:04:05"),
			newest.DateTime.Format("2006-01-02 15:04:05"))
	}
}

// parseSize converts size strings like "9.07T" to bytes
func parseSize(sizeStr string) int64 {
	if sizeStr == "" {
		return 0
	}

	var value float64
	var unit string

	// Parse the number and unit
	n, err := fmt.Sscanf(sizeStr, "%f%s", &value, &unit)
	if err != nil || n < 1 {
		return 0
	}

	// If no unit was parsed (n == 1), treat as bytes
	if n == 1 {
		return int64(value)
	}

	// Convert to bytes based on unit (case-insensitive)
	switch unit {
	case "B", "b":
		return int64(value)
	case "K", "k", "KB", "kb":
		return int64(value * 1024)
	case "M", "m", "MB", "mb":
		return int64(value * 1024 * 1024)
	case "G", "g", "GB", "gb":
		return int64(value * 1024 * 1024 * 1024)
	case "T", "t", "TB", "tb":
		return int64(value * 1024 * 1024 * 1024 * 1024)
	case "P", "p", "PB", "pb":
		return int64(value * 1024 * 1024 * 1024 * 1024 * 1024)
	default:
		// Unknown unit, return as-is
		return int64(value)
	}
}
