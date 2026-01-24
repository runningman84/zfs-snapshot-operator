package config

import (
	"os"
	"testing"
	"time"
)

func TestNewConfig(t *testing.T) {
	tests := []struct {
		name string
		mode string
	}{
		{
			name: "test mode",
			mode: "test",
		},
		{
			name: "direct mode",
			mode: "direct",
		},
		{
			name: "chroot mode",
			mode: "chroot",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := NewConfig(tt.mode)

			if cfg.Mode != tt.mode {
				t.Errorf("Mode = %v, want %v", cfg.Mode, tt.mode)
			}

			// Check default log level
			if cfg.LogLevel != "info" {
				t.Errorf("LogLevel = %v, want info", cfg.LogLevel)
			}

			// Check default values
			if cfg.MaxHourlySnapshots != 24 {
				t.Errorf("MaxHourlySnapshots = %d, want 24", cfg.MaxHourlySnapshots)
			}
			if cfg.MaxDailySnapshots != 7 {
				t.Errorf("MaxDailySnapshots = %d, want 7", cfg.MaxDailySnapshots)
			}
			if cfg.MaxWeeklySnapshots != 4 {
				t.Errorf("MaxWeeklySnapshots = %d, want 4", cfg.MaxWeeklySnapshots)
			}
			if cfg.MaxMonthlySnapshots != 12 {
				t.Errorf("MaxMonthlySnapshots = %d, want 12", cfg.MaxMonthlySnapshots)
			}
			if cfg.MaxYearlySnapshots != 3 {
				t.Errorf("MaxYearlySnapshots = %d, want 3", cfg.MaxYearlySnapshots)
			}

			// Check command initialization
			if len(cfg.ZFSListPoolsCmd) == 0 {
				t.Error("ZFSListPoolsCmd is empty")
			}
			if len(cfg.ZFSListSnapshotsCmd) == 0 {
				t.Error("ZFSListSnapshotsCmd is empty")
			}
			if len(cfg.ZFSCreateSnapshotCmd) == 0 {
				t.Error("ZFSCreateSnapshotCmd is empty")
			}
			if len(cfg.ZFSDeleteSnapshotCmd) == 0 {
				t.Error("ZFSDeleteSnapshotCmd is empty")
			}
			if len(cfg.ZPoolStatusCmd) == 0 {
				t.Error("ZPoolStatusCmd is empty")
			}

			if tt.mode == "test" {
				// Test mode should use test commands
				if cfg.ZFSListPoolsCmd[0] != "cat" {
					t.Errorf("Expected test command 'cat', got '%s'", cfg.ZFSListPoolsCmd[0])
				}
			} else if tt.mode == "chroot" {
				// Chroot mode should use chroot
				if cfg.ZFSListPoolsCmd[0] != "chroot" {
					t.Errorf("Expected chroot command 'chroot', got '%s'", cfg.ZFSListPoolsCmd[0])
				}
			} else if tt.mode == "direct" {
				// Direct mode should use command from PATH
				if cfg.ZFSListPoolsCmd[0] != "zfs" {
					t.Errorf("Expected direct command 'zfs', got '%s'", cfg.ZFSListPoolsCmd[0])
				}
			}
		})
	}
}

func TestIsDebug(t *testing.T) {
	tests := []struct {
		name     string
		logLevel string
		want     bool
	}{
		{
			name:     "debug mode",
			logLevel: "debug",
			want:     true,
		},
		{
			name:     "info mode",
			logLevel: "info",
			want:     false,
		},
		{
			name:     "empty log level",
			logLevel: "",
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := NewConfig("test")
			cfg.LogLevel = tt.logLevel
			if got := cfg.IsDebug(); got != tt.want {
				t.Errorf("IsDebug() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsFilesystemAllowed(t *testing.T) {
	tests := []struct {
		name        string
		whitelist   []string
		filesystem  string
		wantAllowed bool
	}{
		{
			name:        "empty whitelist allows all",
			whitelist:   []string{},
			filesystem:  "tank/data",
			wantAllowed: true,
		},
		{
			name:        "filesystem in whitelist",
			whitelist:   []string{"tank/data", "tank/backup"},
			filesystem:  "tank/data",
			wantAllowed: true,
		},
		{
			name:        "filesystem not in whitelist",
			whitelist:   []string{"tank/data", "tank/backup"},
			filesystem:  "tank/media",
			wantAllowed: false,
		},
		{
			name:        "exact match required",
			whitelist:   []string{"tank/data"},
			filesystem:  "tank/data/subfolder",
			wantAllowed: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := NewConfig("test")
			cfg.FilesystemWhitelist = tt.whitelist
			if got := cfg.IsFilesystemAllowed(tt.filesystem); got != tt.wantAllowed {
				t.Errorf("IsFilesystemAllowed(%s) = %v, want %v", tt.filesystem, got, tt.wantAllowed)
			}
		})
	}
}

func TestGetMaxSnapshotDate(t *testing.T) {
	cfg := NewConfig("test")
	now := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		frequency string
		wantDiff  time.Duration
	}{
		{
			name:      "hourly",
			frequency: "hourly",
			wantDiff:  time.Duration(cfg.MaxHourlySnapshots) * time.Hour,
		},
		{
			name:      "daily",
			frequency: "daily",
			wantDiff:  time.Duration(cfg.MaxDailySnapshots) * 24 * time.Hour,
		},
		{
			name:      "weekly",
			frequency: "weekly",
			wantDiff:  time.Duration(cfg.MaxWeeklySnapshots) * 7 * 24 * time.Hour,
		},
		{
			name:      "monthly",
			frequency: "monthly",
			wantDiff:  time.Duration(cfg.MaxMonthlySnapshots*4) * 7 * 24 * time.Hour,
		},
		{
			name:      "yearly",
			frequency: "yearly",
			wantDiff:  time.Duration(cfg.MaxYearlySnapshots*52) * 7 * 24 * time.Hour,
		},
		{
			name:      "invalid frequency",
			frequency: "invalid",
			wantDiff:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cfg.GetMaxSnapshotDate(tt.frequency, now)
			expected := now.Add(-tt.wantDiff)

			if !result.Equal(expected) {
				t.Errorf("GetMaxSnapshotDate(%s) = %v, want %v", tt.frequency, result, expected)
			}
		})
	}
}

func TestGetMinSnapshotDate(t *testing.T) {
	cfg := NewConfig("test")
	now := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		frequency string
		wantDiff  time.Duration
	}{
		{
			name:      "hourly",
			frequency: "hourly",
			wantDiff:  1 * time.Hour,
		},
		{
			name:      "daily",
			frequency: "daily",
			wantDiff:  24 * time.Hour,
		},
		{
			name:      "weekly",
			frequency: "weekly",
			wantDiff:  7 * 24 * time.Hour,
		},
		{
			name:      "monthly",
			frequency: "monthly",
			wantDiff:  4 * 7 * 24 * time.Hour,
		},
		{
			name:      "yearly",
			frequency: "yearly",
			wantDiff:  52 * 7 * 24 * time.Hour,
		},
		{
			name:      "invalid frequency",
			frequency: "invalid",
			wantDiff:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cfg.GetMinSnapshotDate(tt.frequency, now)
			expected := now.Add(-tt.wantDiff)

			if !result.Equal(expected) {
				t.Errorf("GetMinSnapshotDate(%s) = %v, want %v", tt.frequency, result, expected)
			}
		})
	}
}

func TestFrequencies(t *testing.T) {
	frequencies := Frequencies()

	expectedFrequencies := []string{"hourly", "daily", "weekly", "monthly", "yearly"}

	if len(frequencies) != len(expectedFrequencies) {
		t.Errorf("Frequencies() returned %d items, want %d", len(frequencies), len(expectedFrequencies))
	}

	for i, freq := range expectedFrequencies {
		if frequencies[i] != freq {
			t.Errorf("Frequencies()[%d] = %s, want %s", i, frequencies[i], freq)
		}
	}
}

func TestNewConfigWithEnvironmentVariables(t *testing.T) {
	// Save original environment
	originalEnv := map[string]string{
		"MAX_HOURLY_SNAPSHOTS":  os.Getenv("MAX_HOURLY_SNAPSHOTS"),
		"MAX_DAILY_SNAPSHOTS":   os.Getenv("MAX_DAILY_SNAPSHOTS"),
		"MAX_WEEKLY_SNAPSHOTS":  os.Getenv("MAX_WEEKLY_SNAPSHOTS"),
		"MAX_MONTHLY_SNAPSHOTS": os.Getenv("MAX_MONTHLY_SNAPSHOTS"),
		"MAX_YEARLY_SNAPSHOTS":  os.Getenv("MAX_YEARLY_SNAPSHOTS"),
	}

	// Restore environment after test
	defer func() {
		for key, value := range originalEnv {
			if value == "" {
				os.Unsetenv(key)
			} else {
				os.Setenv(key, value)
			}
		}
	}()

	// Set test environment variables
	os.Setenv("MAX_HOURLY_SNAPSHOTS", "48")
	os.Setenv("MAX_DAILY_SNAPSHOTS", "14")
	os.Setenv("MAX_WEEKLY_SNAPSHOTS", "8")
	os.Setenv("MAX_MONTHLY_SNAPSHOTS", "24")
	os.Setenv("MAX_YEARLY_SNAPSHOTS", "5")

	cfg := NewConfig("test")

	if cfg.MaxHourlySnapshots != 48 {
		t.Errorf("MaxHourlySnapshots = %d, want 48", cfg.MaxHourlySnapshots)
	}
	if cfg.MaxDailySnapshots != 14 {
		t.Errorf("MaxDailySnapshots = %d, want 14", cfg.MaxDailySnapshots)
	}
	if cfg.MaxWeeklySnapshots != 8 {
		t.Errorf("MaxWeeklySnapshots = %d, want 8", cfg.MaxWeeklySnapshots)
	}
	if cfg.MaxMonthlySnapshots != 24 {
		t.Errorf("MaxMonthlySnapshots = %d, want 24", cfg.MaxMonthlySnapshots)
	}
	if cfg.MaxYearlySnapshots != 5 {
		t.Errorf("MaxYearlySnapshots = %d, want 5", cfg.MaxYearlySnapshots)
	}
}

func TestGetEnvAsInt(t *testing.T) {
	tests := []struct {
		name         string
		envValue     string
		defaultValue int
		want         int
	}{
		{
			name:         "valid integer",
			envValue:     "42",
			defaultValue: 10,
			want:         42,
		},
		{
			name:         "empty string",
			envValue:     "",
			defaultValue: 10,
			want:         10,
		},
		{
			name:         "invalid integer",
			envValue:     "not-a-number",
			defaultValue: 10,
			want:         10,
		},
		{
			name:         "zero value",
			envValue:     "0",
			defaultValue: 10,
			want:         0,
		},
		{
			name:         "negative value",
			envValue:     "-5",
			defaultValue: 10,
			want:         -5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up test environment
			testKey := "TEST_ENV_INT_KEY"
			if tt.envValue != "" {
				os.Setenv(testKey, tt.envValue)
			} else {
				os.Unsetenv(testKey)
			}
			defer os.Unsetenv(testKey)

			got := getEnvAsInt(testKey, tt.defaultValue)
			if got != tt.want {
				t.Errorf("getEnvAsInt() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestGetEnvAsStringSlice(t *testing.T) {
	tests := []struct {
		name         string
		envValue     string
		defaultValue []string
		want         []string
	}{
		{
			name:         "single value",
			envValue:     "tank",
			defaultValue: []string{},
			want:         []string{"tank"},
		},
		{
			name:         "multiple values",
			envValue:     "tank,backup,storage",
			defaultValue: []string{},
			want:         []string{"tank", "backup", "storage"},
		},
		{
			name:         "values with spaces",
			envValue:     "tank, backup , storage",
			defaultValue: []string{},
			want:         []string{"tank", "backup", "storage"},
		},
		{
			name:         "empty string",
			envValue:     "",
			defaultValue: []string{"default"},
			want:         []string{"default"},
		},
		{
			name:         "empty values in list",
			envValue:     "tank,,backup",
			defaultValue: []string{},
			want:         []string{"tank", "backup"},
		},
		{
			name:         "only commas",
			envValue:     ",,,",
			defaultValue: []string{"default"},
			want:         []string{"default"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testKey := "TEST_ENV_STRING_SLICE_KEY"
			if tt.envValue != "" {
				os.Setenv(testKey, tt.envValue)
			} else {
				os.Unsetenv(testKey)
			}
			defer os.Unsetenv(testKey)

			got := getEnvAsStringSlice(testKey, tt.defaultValue)
			if len(got) != len(tt.want) {
				t.Errorf("getEnvAsStringSlice() length = %d, want %d", len(got), len(tt.want))
				return
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("getEnvAsStringSlice()[%d] = %s, want %s", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestIsPoolAllowed(t *testing.T) {
	tests := []struct {
		name      string
		whitelist []string
		poolName  string
		want      bool
	}{
		{
			name:      "empty whitelist allows all",
			whitelist: []string{},
			poolName:  "tank",
			want:      true,
		},
		{
			name:      "pool in whitelist",
			whitelist: []string{"tank", "backup"},
			poolName:  "tank",
			want:      true,
		},
		{
			name:      "pool not in whitelist",
			whitelist: []string{"tank", "backup"},
			poolName:  "storage",
			want:      false,
		},
		{
			name:      "single pool whitelist match",
			whitelist: []string{"tank"},
			poolName:  "tank",
			want:      true,
		},
		{
			name:      "single pool whitelist no match",
			whitelist: []string{"tank"},
			poolName:  "backup",
			want:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				PoolWhitelist: tt.whitelist,
			}
			got := cfg.IsPoolAllowed(tt.poolName)
			if got != tt.want {
				t.Errorf("IsPoolAllowed(%s) = %v, want %v", tt.poolName, got, tt.want)
			}
		})
	}
}

func TestNewConfigWithPoolWhitelist(t *testing.T) {
	// Save original environment
	originalEnv := os.Getenv("POOL_WHITELIST")
	defer func() {
		if originalEnv == "" {
			os.Unsetenv("POOL_WHITELIST")
		} else {
			os.Setenv("POOL_WHITELIST", originalEnv)
		}
	}()

	tests := []struct {
		name     string
		envValue string
		want     []string
	}{
		{
			name:     "no whitelist",
			envValue: "",
			want:     []string{},
		},
		{
			name:     "single pool",
			envValue: "tank",
			want:     []string{"tank"},
		},
		{
			name:     "multiple pools",
			envValue: "tank,backup,storage",
			want:     []string{"tank", "backup", "storage"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envValue != "" {
				os.Setenv("POOL_WHITELIST", tt.envValue)
			} else {
				os.Unsetenv("POOL_WHITELIST")
			}

			cfg := NewConfig("test")

			if len(cfg.PoolWhitelist) != len(tt.want) {
				t.Errorf("PoolWhitelist length = %d, want %d", len(cfg.PoolWhitelist), len(tt.want))
				return
			}

			for i := range cfg.PoolWhitelist {
				if cfg.PoolWhitelist[i] != tt.want[i] {
					t.Errorf("PoolWhitelist[%d] = %s, want %s", i, cfg.PoolWhitelist[i], tt.want[i])
				}
			}
		})
	}
}

func TestDryRunEnvironmentVariable(t *testing.T) {
	tests := []struct {
		name     string
		envValue string
		want     bool
	}{
		{
			name:     "dry-run enabled",
			envValue: "true",
			want:     true,
		},
		{
			name:     "dry-run disabled",
			envValue: "false",
			want:     false,
		},
		{
			name:     "dry-run default (not set)",
			envValue: "",
			want:     false,
		},
		{
			name:     "dry-run with 1",
			envValue: "1",
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envValue != "" {
				os.Setenv("DRY_RUN", tt.envValue)
			} else {
				os.Unsetenv("DRY_RUN")
			}

			cfg := NewConfig("test")

			if cfg.DryRun != tt.want {
				t.Errorf("DryRun = %v, want %v", cfg.DryRun, tt.want)
			}

			os.Unsetenv("DRY_RUN")
		})
	}
}

func TestMaxDeletionsPerRunEnvironmentVariable(t *testing.T) {
	tests := []struct {
		name     string
		envValue string
		want     int
	}{
		{
			name:     "default deletion limit",
			envValue: "",
			want:     100,
		},
		{
			name:     "custom deletion limit",
			envValue: "50",
			want:     50,
		},
		{
			name:     "low deletion limit",
			envValue: "1",
			want:     1,
		},
		{
			name:     "high deletion limit",
			envValue: "1000",
			want:     1000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envValue != "" {
				os.Setenv("MAX_DELETIONS_PER_RUN", tt.envValue)
			} else {
				os.Unsetenv("MAX_DELETIONS_PER_RUN")
			}

			cfg := NewConfig("test")

			if cfg.MaxDeletionsPerRun != tt.want {
				t.Errorf("MaxDeletionsPerRun = %d, want %d", cfg.MaxDeletionsPerRun, tt.want)
			}

			os.Unsetenv("MAX_DELETIONS_PER_RUN")
		})
	}
}

func TestLockFilePathEnvironmentVariable(t *testing.T) {
	tests := []struct {
		name     string
		envValue string
		want     string
	}{
		{
			name:     "default lock file path",
			envValue: "",
			want:     "/tmp/zfs-snapshot-operator.lock",
		},
		{
			name:     "custom lock file path",
			envValue: "/var/run/zfs-operator.lock",
			want:     "/var/run/zfs-operator.lock",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envValue != "" {
				os.Setenv("LOCK_FILE_PATH", tt.envValue)
			} else {
				os.Unsetenv("LOCK_FILE_PATH")
			}

			cfg := NewConfig("test")

			if cfg.LockFilePath != tt.want {
				t.Errorf("LockFilePath = %s, want %s", cfg.LockFilePath, tt.want)
			}

			os.Unsetenv("LOCK_FILE_PATH")
		})
	}
}
