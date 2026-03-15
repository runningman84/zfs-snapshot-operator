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
	EnableLocking      bool   // If true, use lock file to prevent concurrent runs (default: true)
	LockFilePath       string // Path to lock file for preventing concurrent runs

	MaxFrequentlySnapshots int
	MaxHourlySnapshots     int
	MaxDailySnapshots      int
	MaxWeeklySnapshots     int
	MaxMonthlySnapshots    int
	MaxYearlySnapshots     int

	// Pool filtering
	PoolWhitelist []string // List of pools to process (empty = all pools)

	// Filesystem filtering
	FilesystemWhitelist []string // List of filesystems to process (empty = all filesystems)

	// Snapshot naming
	SnapshotPrefix string // Prefix for automatic snapshots (default: autosnap)

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
		Mode:                   mode,
		LogLevel:               getEnvAsString("LOG_LEVEL", "info"),
		DryRun:                 getEnvAsBool("DRY_RUN", false),
		MaxDeletionsPerRun:     getEnvAsInt("MAX_DELETIONS_PER_RUN", 100),
		EnableLocking:          getEnvAsBool("ENABLE_LOCKING", true),
		LockFilePath:           getEnvAsString("LOCK_FILE_PATH", "/tmp/zfs-snapshot-operator.lock"),
		MaxFrequentlySnapshots: getEnvAsInt("MAX_FREQUENTLY_SNAPSHOTS", 0),
		MaxHourlySnapshots:     getEnvAsInt("MAX_HOURLY_SNAPSHOTS", 24),
		MaxDailySnapshots:      getEnvAsInt("MAX_DAILY_SNAPSHOTS", 7),
		MaxWeeklySnapshots:     getEnvAsInt("MAX_WEEKLY_SNAPSHOTS", 4),
		MaxMonthlySnapshots:    getEnvAsInt("MAX_MONTHLY_SNAPSHOTS", 12),
		MaxYearlySnapshots:     getEnvAsInt("MAX_YEARLY_SNAPSHOTS", 3),
		PoolWhitelist:          getEnvAsStringSlice("POOL_WHITELIST", []string{}),
		FilesystemWhitelist:    getEnvAsStringSlice("FILESYSTEM_WHITELIST", []string{}),
		SnapshotPrefix:         getEnvAsString("SNAPSHOT_PREFIX", "autosnap"),
		ScrubAgeThresholdDays:  getEnvAsInt("SCRUB_AGE_THRESHOLD_DAYS", 90),
		ChrootHostPath:         getEnvAsString("CHROOT_HOST_PATH", "/host"),
		ChrootBinPath:          getEnvAsString("CHROOT_BIN_PATH", "/usr/local/sbin"),
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

// GetMaxSnapshotsForFrequency returns the maximum number of snapshots to keep for a given frequency
// If filesystemName is provided, it will check for filesystem-specific overrides first
// (e.g., MAX_HOURLY_SNAPSHOTS_TANK_PUBLIC for tank/public filesystem)
func (c *Config) GetMaxSnapshotsForFrequency(frequency string, filesystemName ...string) int {
	var envKey string
	var defaultValue int

	switch frequency {
	case "frequently":
		envKey = "MAX_FREQUENTLY_SNAPSHOTS"
		defaultValue = c.MaxFrequentlySnapshots
	case "hourly":
		envKey = "MAX_HOURLY_SNAPSHOTS"
		defaultValue = c.MaxHourlySnapshots
	case "daily":
		envKey = "MAX_DAILY_SNAPSHOTS"
		defaultValue = c.MaxDailySnapshots
	case "weekly":
		envKey = "MAX_WEEKLY_SNAPSHOTS"
		defaultValue = c.MaxWeeklySnapshots
	case "monthly":
		envKey = "MAX_MONTHLY_SNAPSHOTS"
		defaultValue = c.MaxMonthlySnapshots
	case "yearly":
		envKey = "MAX_YEARLY_SNAPSHOTS"
		defaultValue = c.MaxYearlySnapshots
	default:
		return 0
	}

	// Check for filesystem-specific override if filesystem name is provided
	if len(filesystemName) > 0 && filesystemName[0] != "" {
		if value := getFilesystemSpecificEnvAsInt(envKey, filesystemName[0], -1); value != -1 {
			return value
		}
	}

	return defaultValue
}

// GetMaxSnapshotDate returns the maximum date for a given frequency
// If filesystemName is provided, it will check for filesystem-specific overrides first
func (c *Config) GetMaxSnapshotDate(frequency string, now time.Time, filesystemName ...string) time.Time {
	maxCount := c.GetMaxSnapshotsForFrequency(frequency, filesystemName...)

	switch frequency {
	case "frequently":
		return now.Add(-time.Duration(maxCount) * 15 * time.Minute)
	case "hourly":
		return now.Add(-time.Duration(maxCount) * time.Hour)
	case "daily":
		return now.Add(-time.Duration(maxCount) * 24 * time.Hour)
	case "weekly":
		return now.Add(-time.Duration(maxCount) * 7 * 24 * time.Hour)
	case "monthly":
		return now.Add(-time.Duration(maxCount*4) * 7 * 24 * time.Hour)
	case "yearly":
		return now.Add(-time.Duration(maxCount*52) * 7 * 24 * time.Hour)
	default:
		return now
	}
}

// GetMinSnapshotDate returns the minimum date for a given frequency
func (c *Config) GetMinSnapshotDate(frequency string, now time.Time) time.Time {
	switch frequency {
	case "frequently":
		return now.Add(-15 * time.Minute)
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
	return []string{"frequently", "hourly", "daily", "weekly", "monthly", "yearly"}
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

// getFilesystemSpecificEnvAsInt checks for a filesystem-specific environment variable
// For example, for filesystem "tank/public" and key "MAX_HOURLY_SNAPSHOTS",
// it will look for "MAX_HOURLY_SNAPSHOTS_TANK_PUBLIC"
func getFilesystemSpecificEnvAsInt(key string, filesystemName string, defaultValue int) int {
	// Convert filesystem name to env var suffix
	// Replace "/" with "_" and convert to uppercase
	suffix := strings.ToUpper(strings.ReplaceAll(filesystemName, "/", "_"))
	specificKey := key + "_" + suffix

	valueStr := os.Getenv(specificKey)
	if valueStr == "" {
		return defaultValue
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return defaultValue
	}

	return value
}
