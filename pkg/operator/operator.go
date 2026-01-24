package operator

import (
	"fmt"
	"log"
	"time"

	"github.com/runningman84/zfs-snapshot-operator/pkg/config"
	"github.com/runningman84/zfs-snapshot-operator/pkg/models"
	"github.com/runningman84/zfs-snapshot-operator/pkg/zfs"
)

// Operator manages ZFS snapshot operations
type Operator struct {
	config  *config.Config
	manager *zfs.Manager
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
	now := time.Now()

	o.logConfig(now)

	// Get and log ZFS version information
	userland, kernel, err := o.manager.GetVersion()
	if err != nil {
		log.Printf("Warning: failed to get ZFS version: %v", err)
	} else {
		log.Printf("ZFS Version - Userland: %s, Kernel: %s", userland, kernel)
	}

	// Get pool health status first
	poolStatus, err := o.manager.GetPoolStatus()
	if err != nil {
		return fmt.Errorf("failed to get pool status: %w", err)
	}

	pools, err := o.manager.GetPools()
	if err != nil {
		return fmt.Errorf("failed to get pools: %w", err)
	}

	for _, pool := range pools {
		if err := o.processPool(pool, now, poolStatus); err != nil {
			log.Printf("Error processing pool %s: %v", pool.PoolName, err)
		}
	}

	return nil
}

func (o *Operator) logConfig(now time.Time) {
	log.Println("Current config")
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
	log.Printf("Processing pool %s", pool.PoolName)

	// Check if pool is in whitelist
	if !o.config.IsPoolAllowed(pool.PoolName) {
		log.Printf("Skipping pool %s (not in whitelist)", pool.PoolName)
		return nil
	}

	// Check pool health before any operations
	if !o.manager.IsPoolHealthy(pool.PoolName, poolStatus) {
		log.Printf("Skipping pool %s due to health issues", pool.PoolName)
		return fmt.Errorf("pool %s is not healthy", pool.PoolName)
	}

	// Check if scrub is older than 3 months
	o.checkScrubAge(pool.PoolName, poolStatus, now)

	if pool.FilesystemName == "" {
		log.Printf("Ignoring pool without filesystem %s", pool.PoolName)
		return nil
	}

	log.Printf("Processing filesystem %s", pool.FilesystemName)

	for _, frequency := range config.Frequencies() {
		if err := o.processFrequency(pool, frequency, now); err != nil {
			log.Printf("Error processing frequency %s: %v", frequency, err)
		}
	}

	log.Printf("Finished filesystem %s", pool.FilesystemName)
	log.Printf("Finished pool %s", pool.PoolName)

	return nil
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
	}
}

func (o *Operator) processFrequency(pool *models.Pool, frequency string, now time.Time) error {
	log.Printf("Processing frequency %s", frequency)

	snapshots, err := o.manager.GetSnapshots(pool.PoolName, pool.FilesystemName, frequency)
	if err != nil {
		return fmt.Errorf("failed to get snapshots: %w", err)
	}

	var snapshotRecent *models.Snapshot
	for _, snapshot := range snapshots {
		if o.manager.CanSnapshotBeDeleted(snapshot, frequency, now) {
			if err := o.manager.DeleteSnapshot(snapshot); err != nil {
				log.Printf("Failed to delete snapshot: %v", err)
			}
		} else {
			log.Printf("Keeping snapshot %s", snapshot.SnapshotName)
		}

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

		if err := o.manager.CreateSnapshot(newSnapshot); err != nil {
			return fmt.Errorf("failed to create snapshot: %w", err)
		}
	}

	return nil
}
