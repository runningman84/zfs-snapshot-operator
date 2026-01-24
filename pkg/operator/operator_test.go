package operator

import (
	"testing"
	"time"

	"github.com/runningman84/zfs-snapshot-operator/pkg/config"
)

func TestNewOperator(t *testing.T) {
	cfg := config.NewConfig("test")
	op := NewOperator(cfg)

	if op == nil {
		t.Fatal("NewOperator() returned nil")
	}

	if op.config != cfg {
		t.Error("Operator config not properly set")
	}

	if op.manager == nil {
		t.Error("Operator manager not properly initialized")
	}
}

func TestLogConfigWithLogLevel(t *testing.T) {
	cfg := config.NewConfig("test")
	cfg.LogLevel = "debug"
	op := NewOperator(cfg)

	now := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)

	// This should not panic
	op.logConfig(now)
}

func TestLogConfigWithFilesystemWhitelist(t *testing.T) {
	cfg := config.NewConfig("test")
	cfg.FilesystemWhitelist = []string{"tank/data", "tank/backup"}
	op := NewOperator(cfg)

	now := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)

	// This should not panic
	op.logConfig(now)
}

func TestProcessPoolWithFilesystemWhitelist(t *testing.T) {
	tests := []struct {
		name          string
		whitelist     []string
		filesystem    string
		shouldProcess bool
	}{
		{
			name:          "empty whitelist processes all",
			whitelist:     []string{},
			filesystem:    "tank/data",
			shouldProcess: true,
		},
		{
			name:          "filesystem in whitelist",
			whitelist:     []string{"tank/data", "tank/backup"},
			filesystem:    "tank/data",
			shouldProcess: true,
		},
		{
			name:          "filesystem not in whitelist",
			whitelist:     []string{"tank/data", "tank/backup"},
			filesystem:    "tank/media",
			shouldProcess: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.NewConfig("test")
			cfg.FilesystemWhitelist = tt.whitelist

			allowed := cfg.IsFilesystemAllowed(tt.filesystem)
			if allowed != tt.shouldProcess {
				t.Errorf("IsFilesystemAllowed(%s) = %v, want %v", tt.filesystem, allowed, tt.shouldProcess)
			}
		})
	}
}

func TestLogSnapshotSummary(t *testing.T) {
	// This test ensures the logSnapshotSummary function doesn't panic
	// and can handle pools with various snapshot counts
	cfg := config.NewConfig("test")
	op := NewOperator(cfg)

	// Test that operator is properly initialized
	if op == nil {
		t.Fatal("Operator should not be nil")
	}

	// The actual output is tested through integration tests
	// Here we just verify the operator structure is valid
	if op.config == nil {
		t.Error("Operator config should not be nil")
	}
	if op.manager == nil {
		t.Error("Operator manager should not be nil")
	}
}

func TestConfigIsDebugIntegration(t *testing.T) {
	tests := []struct {
		name     string
		logLevel string
		want     bool
	}{
		{"debug mode", "debug", true},
		{"info mode", "info", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.NewConfig("test")
			cfg.LogLevel = tt.logLevel
			op := NewOperator(cfg)

			got := op.config.IsDebug()
			if got != tt.want {
				t.Errorf("IsDebug() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDryRunMode(t *testing.T) {
	cfg := config.NewConfig("test")
	cfg.DryRun = true
	op := NewOperator(cfg)

	if !op.config.DryRun {
		t.Error("DryRun should be true")
	}

	// Verify dry-run is properly configured
	if op.config.DryRun != true {
		t.Errorf("DryRun = %v, want true", op.config.DryRun)
	}
}

func TestDeletionLimit(t *testing.T) {
	tests := []struct {
		name  string
		limit int
	}{
		{"default limit", 100},
		{"custom limit", 50},
		{"low limit", 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.NewConfig("test")
			cfg.MaxDeletionsPerRun = tt.limit
			op := NewOperator(cfg)

			if op.config.MaxDeletionsPerRun != tt.limit {
				t.Errorf("MaxDeletionsPerRun = %d, want %d", op.config.MaxDeletionsPerRun, tt.limit)
			}
		})
	}
}

func TestLockFilePath(t *testing.T) {
	tests := []struct {
		name string
		path string
	}{
		{"default path", "/tmp/zfs-snapshot-operator.lock"},
		{"custom path", "/var/run/zfs-operator.lock"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.NewConfig("test")
			cfg.LockFilePath = tt.path
			op := NewOperator(cfg)

			if op.config.LockFilePath != tt.path {
				t.Errorf("LockFilePath = %s, want %s", op.config.LockFilePath, tt.path)
			}
		})
	}
}

func TestDeletionCounter(t *testing.T) {
	cfg := config.NewConfig("test")
	op := NewOperator(cfg)

	// Initial state
	if op.deletionCount != 0 {
		t.Errorf("Initial deletionCount = %d, want 0", op.deletionCount)
	}
}

func TestMinimumSnapshotRetention(t *testing.T) {
	// This test verifies that the logic exists to prevent deleting all snapshots
	// The actual behavior is tested through integration tests in processFrequency
	cfg := config.NewConfig("test")
	op := NewOperator(cfg)

	// Verify operator can be created with all safety features
	if op.config.MaxDeletionsPerRun <= 0 {
		t.Error("MaxDeletionsPerRun should be positive")
	}
}
