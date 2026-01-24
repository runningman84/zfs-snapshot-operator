package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds the application configuration
type Config struct {
	TestMode bool

	MaxHourlySnapshots  int
	MaxDailySnapshots   int
	MaxWeeklySnapshots  int
	MaxMonthlySnapshots int
	MaxYearlySnapshots  int

	// Pool filtering
	PoolWhitelist []string // List of pools to process (empty = all pools)

	// Scrub monitoring
	ScrubAgeThresholdDays int // Number of days before warning about old scrubs

	// Commands
	ZFSListPoolsCmd      []string
	ZFSListSnapshotsCmd  []string
	ZFSCreateSnapshotCmd []string
	ZFSDeleteSnapshotCmd []string
	ZPoolStatusCmd       []string
}

// NewConfig creates a new configuration with default values
func NewConfig(testMode bool) *Config {
	cfg := &Config{
		TestMode:              testMode,
		MaxHourlySnapshots:    getEnvAsInt("MAX_HOURLY_SNAPSHOTS", 24),
		MaxDailySnapshots:     getEnvAsInt("MAX_DAILY_SNAPSHOTS", 7),
		MaxWeeklySnapshots:    getEnvAsInt("MAX_WEEKLY_SNAPSHOTS", 4),
		MaxMonthlySnapshots:   getEnvAsInt("MAX_MONTHLY_SNAPSHOTS", 12),
		MaxYearlySnapshots:    getEnvAsInt("MAX_YEARLY_SNAPSHOTS", 3),
		PoolWhitelist:         getEnvAsStringSlice("POOL_WHITELIST", []string{}),
		ScrubAgeThresholdDays: getEnvAsInt("SCRUB_AGE_THRESHOLD_DAYS", 90),
	}

	if testMode {
		cfg.ZFSListPoolsCmd = []string{"cat", "test/zfs_list_pools.json"}
		cfg.ZFSListSnapshotsCmd = []string{"cat", "test/zfs_list_snapshots.json"}
		cfg.ZFSCreateSnapshotCmd = []string{"true"}
		cfg.ZFSDeleteSnapshotCmd = []string{"true"}
		cfg.ZPoolStatusCmd = []string{"cat", "test/zpool_status.json"}
	} else {
		zfsBin := []string{"chroot", "/host", "/usr/local/sbin/zfs"}
		zpoolBin := []string{"chroot", "/host", "/usr/local/sbin/zpool"}
		cfg.ZFSListPoolsCmd = append(zfsBin, "list", "-j")
		cfg.ZFSListSnapshotsCmd = append(zfsBin, "list", "-j", "-t", "snapshot")
		cfg.ZFSCreateSnapshotCmd = append(zfsBin, "snapshot")
		cfg.ZFSDeleteSnapshotCmd = append(zfsBin, "destroy")
		cfg.ZPoolStatusCmd = append(zpoolBin, "status", "-j")
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
