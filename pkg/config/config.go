package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds the application configuration
type Config struct {
	Mode     string // Operation mode: test, direct, or chroot
	LogLevel string // Log level: info or debug

	// Safety features
	DryRun             bool   // If true, log deletions but don't actually delete
	MaxDeletionsPerRun int    // Maximum snapshots to delete in one run
	LockFilePath       string // Path to lock file for preventing concurrent runs

	MaxHourlySnapshots  int
	MaxDailySnapshots   int
	MaxWeeklySnapshots  int
	MaxMonthlySnapshots int
	MaxYearlySnapshots  int

	// Pool filtering
	PoolWhitelist []string // List of pools to process (empty = all pools)

	// Filesystem filtering
	FilesystemWhitelist []string // List of filesystems to process (empty = all filesystems)

	// Scrub monitoring
	ScrubAgeThresholdDays int // Number of days before warning about old scrubs

	// Chroot configuration
	ChrootHostPath string // Path to host root for chroot mode (default: /host)
	ChrootBinPath  string // Path to ZFS binaries in chroot mode (default: /usr/local/sbin)

	// Commands
	ZFSListPoolsCmd      []string
	ZFSListSnapshotsCmd  []string
	ZFSCreateSnapshotCmd []string
	ZFSDeleteSnapshotCmd []string
	ZPoolStatusCmd       []string
	ZPoolVersionCmd      []string
	ZFSVersionCmd        []string
}

// NewConfig creates a new configuration with default values
// mode can be: "test" (use test files), "direct" (no chroot), "chroot" (production with chroot)
func NewConfig(mode string) *Config {
	cfg := &Config{
		Mode:                  mode,
		LogLevel:              getEnvAsString("LOG_LEVEL", "info"),
		DryRun:                getEnvAsBool("DRY_RUN", false),
		MaxDeletionsPerRun:    getEnvAsInt("MAX_DELETIONS_PER_RUN", 100),
		LockFilePath:          getEnvAsString("LOCK_FILE_PATH", "/tmp/zfs-snapshot-operator.lock"),
		MaxHourlySnapshots:    getEnvAsInt("MAX_HOURLY_SNAPSHOTS", 24),
		MaxDailySnapshots:     getEnvAsInt("MAX_DAILY_SNAPSHOTS", 7),
		MaxWeeklySnapshots:    getEnvAsInt("MAX_WEEKLY_SNAPSHOTS", 4),
		MaxMonthlySnapshots:   getEnvAsInt("MAX_MONTHLY_SNAPSHOTS", 12),
		MaxYearlySnapshots:    getEnvAsInt("MAX_YEARLY_SNAPSHOTS", 3),
		PoolWhitelist:         getEnvAsStringSlice("POOL_WHITELIST", []string{}),
		FilesystemWhitelist:   getEnvAsStringSlice("FILESYSTEM_WHITELIST", []string{}),
		ScrubAgeThresholdDays: getEnvAsInt("SCRUB_AGE_THRESHOLD_DAYS", 90),
		ChrootHostPath:        getEnvAsString("CHROOT_HOST_PATH", "/host"),
		ChrootBinPath:         getEnvAsString("CHROOT_BIN_PATH", "/usr/local/sbin"),
	}

	switch mode {
	case "test":
		// Use test files for testing
		cfg.ZFSListPoolsCmd = []string{"cat", "test/zfs_list_pools.json"}
		cfg.ZFSListSnapshotsCmd = []string{"cat", "test/zfs_list_snapshots.json"}
		cfg.ZFSCreateSnapshotCmd = []string{"true"}
		cfg.ZFSDeleteSnapshotCmd = []string{"true"}
		cfg.ZPoolStatusCmd = []string{"cat", "test/zpool_status.json"}
		cfg.ZPoolVersionCmd = []string{"cat", "test/zpool_version.json"}
		cfg.ZFSVersionCmd = []string{"cat", "test/zfs_version.json"}
	case "direct":
		// Direct access without chroot (e.g., for local development)
		// Uses zfs and zpool from $PATH
		cfg.ZFSListPoolsCmd = []string{"zfs", "list", "-j"}
		cfg.ZFSListSnapshotsCmd = []string{"zfs", "list", "-j", "-t", "snapshot"}
		cfg.ZFSCreateSnapshotCmd = []string{"zfs", "snapshot"}
		cfg.ZFSDeleteSnapshotCmd = []string{"zfs", "destroy"}
		cfg.ZPoolStatusCmd = []string{"zpool", "status", "-j"}
		cfg.ZPoolVersionCmd = []string{"zpool", "version", "-j"}
		cfg.ZFSVersionCmd = []string{"zfs", "version", "-j"}
	case "chroot":
		// Production mode with chroot to access host ZFS
		zfsBin := []string{"chroot", cfg.ChrootHostPath, cfg.ChrootBinPath + "/zfs"}
		zpoolBin := []string{"chroot", cfg.ChrootHostPath, cfg.ChrootBinPath + "/zpool"}
		cfg.ZFSListPoolsCmd = append(zfsBin, "list", "-j")
		cfg.ZFSListSnapshotsCmd = append(zfsBin, "list", "-j", "-t", "snapshot")
		cfg.ZFSCreateSnapshotCmd = append(zfsBin, "snapshot")
		cfg.ZFSDeleteSnapshotCmd = append(zfsBin, "destroy")
		cfg.ZPoolStatusCmd = append(zpoolBin, "status", "-j")
		cfg.ZPoolVersionCmd = append(zpoolBin, "version", "-j")
		cfg.ZFSVersionCmd = append(zfsBin, "version", "-j")
	}

	return cfg
}

// GetMaxSnapshotDate returns the maximum date for a given frequency
func (c *Config) GetMaxSnapshotDate(frequency string, now time.Time) time.Time {
	switch frequency {
	case "hourly":
		return now.Add(-time.Duration(c.MaxHourlySnapshots) * time.Hour)
	case "daily":
		return now.Add(-time.Duration(c.MaxDailySnapshots) * 24 * time.Hour)
	case "weekly":
		return now.Add(-time.Duration(c.MaxWeeklySnapshots) * 7 * 24 * time.Hour)
	case "monthly":
		return now.Add(-time.Duration(c.MaxMonthlySnapshots*4) * 7 * 24 * time.Hour)
	case "yearly":
		return now.Add(-time.Duration(c.MaxYearlySnapshots*52) * 7 * 24 * time.Hour)
	default:
		return now
	}
}

// GetMinSnapshotDate returns the minimum date for a given frequency
func (c *Config) GetMinSnapshotDate(frequency string, now time.Time) time.Time {
	switch frequency {
	case "hourly":
		return now.Add(-1 * time.Hour)
	case "daily":
		return now.Add(-24 * time.Hour)
	case "weekly":
		return now.Add(-7 * 24 * time.Hour)
	case "monthly":
		return now.Add(-4 * 7 * 24 * time.Hour)
	case "yearly":
		return now.Add(-52 * 7 * 24 * time.Hour)
	default:
		return now
	}
}

// Frequencies returns the list of supported snapshot frequencies
func Frequencies() []string {
	return []string{"hourly", "daily", "weekly", "monthly", "yearly"}
}

// getEnvAsInt reads an environment variable and returns it as an integer,
// or returns the default value if not set or invalid
func getEnvAsInt(key string, defaultValue int) int {
	valueStr := os.Getenv(key)
	if valueStr == "" {
		return defaultValue
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return defaultValue
	}

	return value
}

// getEnvAsStringSlice reads an environment variable as a comma-separated list,
// or returns the default value if not set
func getEnvAsStringSlice(key string, defaultValue []string) []string {
	valueStr := os.Getenv(key)
	if valueStr == "" {
		return defaultValue
	}

	// Split by comma and trim whitespace
	parts := strings.Split(valueStr, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}

	if len(result) == 0 {
		return defaultValue
	}

	return result
}

// getEnvAsString gets an environment variable as a string,
// or returns the default value if not set
func getEnvAsString(key string, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

// IsPoolAllowed checks if a pool is in the whitelist (or if whitelist is empty, all pools are allowed)
func (c *Config) IsPoolAllowed(poolName string) bool {
	// If whitelist is empty, all pools are allowed
	if len(c.PoolWhitelist) == 0 {
		return true
	}

	// Check if pool is in whitelist
	for _, allowedPool := range c.PoolWhitelist {
		if allowedPool == poolName {
			return true
		}
	}

	return false
}

// IsDebug returns true if log level is set to debug
func (c *Config) IsDebug() bool {
	return c.LogLevel == "debug"
}

// IsFilesystemAllowed checks if a filesystem is in the whitelist (or if whitelist is empty, all filesystems are allowed)
func (c *Config) IsFilesystemAllowed(filesystemName string) bool {
	// If whitelist is empty, all filesystems are allowed
	if len(c.FilesystemWhitelist) == 0 {
		return true
	}

	// Check if filesystem is in whitelist
	for _, allowedFilesystem := range c.FilesystemWhitelist {
		if allowedFilesystem == filesystemName {
			return true
		}
	}

	return false
}

// getEnvAsBool gets an environment variable as a boolean
func getEnvAsBool(key string, defaultValue bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	boolValue, err := strconv.ParseBool(value)
	if err != nil {
		return defaultValue
	}
	return boolValue
}
