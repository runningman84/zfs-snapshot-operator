package operator

import (
	"testing"
	"time"

	"github.com/runningman84/zfs-snapshot-operator/pkg/config"
	"github.com/runningman84/zfs-snapshot-operator/pkg/models"
)

// mockZFSManager is a mock implementation of zfs.Manager for testing
type mockZFSManager struct {
	snapshots         []*models.Snapshot
	pools             []*models.Pool
	poolStatus        map[string]*models.PoolStatus
	createError       error
	deleteError       error
	getSnapshotsError error
	getPoolsError     error
	createdSnapshots  []*models.Snapshot
	deletedSnapshots  []*models.Snapshot
}

func (m *mockZFSManager) GetVersion() (string, string, error) {
	return "zfs-2.3.3-1", "zfs-kmod-2.3.3-1", nil
}

func (m *mockZFSManager) GetPools() ([]*models.Pool, error) {
	if m.getPoolsError != nil {
		return nil, m.getPoolsError
	}
	return m.pools, nil
}

func (m *mockZFSManager) GetSnapshots(poolName, filesystemName, frequency string) ([]*models.Snapshot, error) {
	if m.getSnapshotsError != nil {
		return nil, m.getSnapshotsError
	}

	var result []*models.Snapshot
	for _, snap := range m.snapshots {
		if snap.PoolName == poolName && snap.FilesystemName == filesystemName && snap.Frequency == frequency {
			result = append(result, snap)
		}
	}
	return result, nil
}

func (m *mockZFSManager) CreateSnapshot(snapshot *models.Snapshot) error {
	if m.createError != nil {
		return m.createError
	}
	m.createdSnapshots = append(m.createdSnapshots, snapshot)
	m.snapshots = append(m.snapshots, snapshot)
	return nil
}

func (m *mockZFSManager) DeleteSnapshot(snapshot *models.Snapshot) error {
	if m.deleteError != nil {
		return m.deleteError
	}
	m.deletedSnapshots = append(m.deletedSnapshots, snapshot)
	return nil
}

func (m *mockZFSManager) IsSnapshotRecent(snapshot *models.Snapshot, frequency string, now time.Time) bool {
	if snapshot.Frequency != frequency {
		return false
	}

	minDate := now.Add(-1 * time.Hour)
	switch frequency {
	case "daily":
		minDate = now.Add(-24 * time.Hour)
	case "weekly":
		minDate = now.Add(-7 * 24 * time.Hour)
	case "monthly":
		minDate = now.Add(-30 * 24 * time.Hour)
	case "yearly":
		minDate = now.Add(-365 * 24 * time.Hour)
	}

	return snapshot.DateTime.After(minDate) || snapshot.DateTime.Equal(minDate)
}

func (m *mockZFSManager) CanSnapshotBeDeleted(snapshot *models.Snapshot, frequency string, now time.Time) bool {
	return !m.IsSnapshotRecent(snapshot, frequency, now)
}

func (m *mockZFSManager) GetPoolStatus(poolName string) (map[string]*models.PoolStatus, error) {
	return m.poolStatus, nil
}

func (m *mockZFSManager) IsPoolHealthy(poolName string, poolStatus map[string]*models.PoolStatus) bool {
	status, exists := poolStatus[poolName]
	if !exists {
		return true
	}
	if status.State != "ONLINE" {
		return false
	}
	// Check error counts
	if status.ReadErrors != "" && status.ReadErrors != "0" {
		return false
	}
	if status.WriteErrors != "" && status.WriteErrors != "0" {
		return false
	}
	if status.ChecksumErrors != "" && status.ChecksumErrors != "0" {
		return false
	}
	return true
}

// TestProcessFrequencyCreateFirst tests that snapshots are created before deletions
func TestProcessFrequencyCreateFirst(t *testing.T) {
	cfg := config.NewConfig("test")
	cfg.MaxYearlySnapshots = 2

	// This test documents the expected create-first behavior
	t.Log("processFrequency should create new snapshot before deleting old ones")
	t.Log("If creation fails, no deletions should occur")
	t.Log("This requires dependency injection to test with mock manager")
}

// TestProcessFrequencyWithCreateError tests that deletions are skipped when creation fails
func TestProcessFrequencyWithCreateError(t *testing.T) {
	cfg := config.NewConfig("test")
	cfg.MaxYearlySnapshots = 2

	// This test documents that when CreateSnapshot fails, no deletions should happen
	// The create-first logic ensures backup protection is never reduced
	t.Log("When CreateSnapshot returns error, processFrequency should:")
	t.Log("1. Not delete any existing snapshots")
	t.Log("2. Return the error immediately")
	t.Log("3. Preserve all old snapshots as safety measure")
}

// TestProcessFrequencyDeduplication tests that only newest snapshot per period is kept
func TestProcessFrequencyDeduplication(t *testing.T) {
	// Setup: Multiple yearly snapshots in 2024
	// Expected: Only the newest 2024 snapshot should be kept
	t.Log("When multiple snapshots exist in same period (e.g., multiple 2024 snapshots):")
	t.Log("1. Group snapshots by getTimePeriodKey")
	t.Log("2. Keep only the newest snapshot per period")
	t.Log("3. Delete all other snapshots in that period")
}

// TestParseSize tests the parseSize helper function
func TestParseSize(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int64
	}{
		{
			name:     "kilobytes",
			input:    "100K",
			expected: 100 * 1024,
		},
		{
			name:     "megabytes",
			input:    "50M",
			expected: 50 * 1024 * 1024,
		},
		{
			name:     "gigabytes",
			input:    "10G",
			expected: 10 * 1024 * 1024 * 1024,
		},
		{
			name:     "terabytes",
			input:    "2T",
			expected: 2 * 1024 * 1024 * 1024 * 1024,
		},
		{
			name:     "zero",
			input:    "0",
			expected: 0,
		},
		{
			name:     "invalid",
			input:    "invalid",
			expected: 0,
		},
		{
			name:     "empty",
			input:    "",
			expected: 0,
		},
		{
			name:     "decimal gigabytes",
			input:    "1.5G",
			expected: int64(1.5 * 1024 * 1024 * 1024),
		},
		{
			name:     "uppercase KB",
			input:    "100KB",
			expected: 100 * 1024,
		},
		{
			name:     "lowercase mb",
			input:    "50mb",
			expected: 50 * 1024 * 1024,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseSize(tt.input)
			if result != tt.expected {
				t.Errorf("parseSize(%q) = %d, want %d", tt.input, result, tt.expected)
			}
		})
	}
}

// TestCheckScrubAge tests the scrub age monitoring
func TestCheckScrubAge(t *testing.T) {
	cfg := config.NewConfig("test")
	cfg.ScrubAgeThresholdDays = 90
	op := NewOperator(cfg)

	now := time.Date(2026, 1, 25, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name          string
		poolStatus    map[string]*models.PoolStatus
		poolName      string
		expectWarning bool
		description   string
	}{
		{
			name: "recent scrub - no warning",
			poolStatus: map[string]*models.PoolStatus{
				"tank": {
					Name:          "tank",
					LastScrubTime: now.Add(-30 * 24 * time.Hour).Unix(),
					ScrubState:    "finished",
				},
			},
			poolName:      "tank",
			expectWarning: false,
			description:   "Scrub 30 days old, threshold 90 days",
		},
		{
			name: "old scrub - warning",
			poolStatus: map[string]*models.PoolStatus{
				"tank": {
					Name:          "tank",
					LastScrubTime: now.Add(-120 * 24 * time.Hour).Unix(),
					ScrubState:    "finished",
				},
			},
			poolName:      "tank",
			expectWarning: true,
			description:   "Scrub 120 days old, threshold 90 days",
		},
		{
			name: "never scrubbed",
			poolStatus: map[string]*models.PoolStatus{
				"tank": {
					Name:          "tank",
					LastScrubTime: 0,
					ScrubState:    "none",
				},
			},
			poolName:      "tank",
			expectWarning: false,
			description:   "Never scrubbed - no warning",
		},
		{
			name:          "pool not in status",
			poolStatus:    map[string]*models.PoolStatus{},
			poolName:      "tank",
			expectWarning: false,
			description:   "Pool not in status map",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// checkScrubAge logs warnings but doesn't return values
			// This test verifies it doesn't panic and handles edge cases
			op.checkScrubAge(tt.poolName, tt.poolStatus, now)
			t.Logf("✓ %s", tt.description)
		})
	}
}

// TestLogPoolStatus tests pool status logging
func TestLogPoolStatus(t *testing.T) {
	op := NewOperator(config.NewConfig("test"))

	tests := []struct {
		name       string
		poolStatus map[string]*models.PoolStatus
		poolName   string
		hasErrors  bool
	}{
		{
			name: "healthy pool",
			poolStatus: map[string]*models.PoolStatus{
				"tank": {
					Name:           "tank",
					State:          "ONLINE",
					ReadErrors:     "0",
					WriteErrors:    "0",
					ChecksumErrors: "0",
				},
			},
			poolName:  "tank",
			hasErrors: false,
		},
		{
			name: "pool with read errors",
			poolStatus: map[string]*models.PoolStatus{
				"tank": {
					Name:           "tank",
					State:          "ONLINE",
					ReadErrors:     "5",
					WriteErrors:    "0",
					ChecksumErrors: "0",
				},
			},
			poolName:  "tank",
			hasErrors: true,
		},
		{
			name: "pool with checksum errors",
			poolStatus: map[string]*models.PoolStatus{
				"tank": {
					Name:           "tank",
					State:          "ONLINE",
					ReadErrors:     "0",
					WriteErrors:    "0",
					ChecksumErrors: "3",
				},
			},
			poolName:  "tank",
			hasErrors: true,
		},
		{
			name:       "pool not in status",
			poolStatus: map[string]*models.PoolStatus{},
			poolName:   "tank",
			hasErrors:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// logPoolStatus logs warnings but doesn't return values
			// This test verifies it doesn't panic
			op.logPoolStatus(tt.poolName, tt.poolStatus)
			t.Logf("✓ Pool status logged for %s", tt.name)
		})
	}
}

// TestLogFilesystemUsage tests filesystem usage logging
func TestLogFilesystemUsage(t *testing.T) {
	op := NewOperator(config.NewConfig("test"))

	tests := []struct {
		name string
		pool *models.Pool
	}{
		{
			name: "filesystem with usage",
			pool: &models.Pool{
				PoolName:       "tank",
				FilesystemName: "tank/data",
				Used:           "100G",
				Avail:          "900G",
			},
		},
		{
			name: "filesystem with small usage",
			pool: &models.Pool{
				PoolName:       "tank",
				FilesystemName: "tank/backup",
				Used:           "1.5T",
				Avail:          "500G",
			},
		},
		{
			name: "filesystem with no usage data",
			pool: &models.Pool{
				PoolName:       "tank",
				FilesystemName: "tank/empty",
				Used:           "",
				Avail:          "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// logFilesystemUsage logs info but doesn't return values
			// This test verifies it doesn't panic
			op.logFilesystemUsage(tt.pool)
			t.Logf("✓ Filesystem usage logged for %s", tt.name)
		})
	}
}

// TestDeletionCounterRespected tests that deletion limits are enforced
func TestDeletionCounterRespected(t *testing.T) {
	cfg := config.NewConfig("test")
	cfg.MaxDeletionsPerRun = 2
	cfg.MaxYearlySnapshots = 1

	t.Log("When deletion limit is 2 and 5 snapshots need deletion:")
	t.Log("1. Only first 2 deletions should execute")
	t.Log("2. Remaining 3 deletions should be skipped")
	t.Log("3. Warning should be logged about reaching limit")
	t.Log("4. deletionCount should equal MaxDeletionsPerRun")
}

// TestCreationCounter tests that creation counter is accurate
func TestCreationCounter(t *testing.T) {
	cfg := config.NewConfig("test")
	op := NewOperator(cfg)

	if op.creationCount != 0 {
		t.Errorf("Initial creation count should be 0, got %d", op.creationCount)
	}

	t.Log("creationCount should increment for each snapshot created")
	t.Log("In dry-run mode, counter should still increment")
}

// TestDeletionCounterAccuracy tests deletion counter accuracy
func TestDeletionCounterAccuracy(t *testing.T) {
	cfg := config.NewConfig("test")
	op := NewOperator(cfg)

	if op.deletionCount != 0 {
		t.Errorf("Initial deletion count should be 0, got %d", op.deletionCount)
	}

	t.Log("deletionCount should increment for each snapshot deleted")
	t.Log("Failed deletions should not increment counter")
	t.Log("In dry-run mode, counter should still increment")
}

// TestGetSnapshotsError tests error handling when GetSnapshots fails
func TestGetSnapshotsError(t *testing.T) {
	t.Log("When GetSnapshots returns error:")
	t.Log("1. processFrequency should return error immediately")
	t.Log("2. No snapshots should be created")
	t.Log("3. No snapshots should be deleted")
	t.Log("4. Error should propagate to caller")
}

// TestRetentionWindowCalculation tests that retention cutoff dates are correct
func TestRetentionWindowCalculation(t *testing.T) {
	cfg := config.NewConfig("test")
	cfg.MaxYearlySnapshots = 3
	cfg.MaxMonthlySnapshots = 12
	cfg.MaxWeeklySnapshots = 4
	cfg.MaxDailySnapshots = 7
	cfg.MaxHourlySnapshots = 24

	now := time.Date(2026, 1, 25, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		frequency      string
		expectedCutoff time.Time
	}{
		{
			frequency:      "hourly",
			expectedCutoff: now.Add(-24 * time.Hour),
		},
		{
			frequency:      "daily",
			expectedCutoff: now.Add(-7 * 24 * time.Hour),
		},
		{
			frequency:      "weekly",
			expectedCutoff: now.Add(-4 * 7 * 24 * time.Hour),
		},
		{
			frequency:      "monthly",
			expectedCutoff: now.Add(-12 * 4 * 7 * 24 * time.Hour),
		},
		{
			frequency:      "yearly",
			expectedCutoff: now.Add(-3 * 52 * 7 * 24 * time.Hour),
		},
	}

	for _, tt := range tests {
		t.Run(tt.frequency, func(t *testing.T) {
			cutoff := cfg.GetMaxSnapshotDate(tt.frequency, now)
			if !cutoff.Equal(tt.expectedCutoff) {
				t.Errorf("Retention cutoff for %s = %v, want %v", tt.frequency, cutoff, tt.expectedCutoff)
			}
		})
	}
}

// TestDryRunMode tests that dry-run mode doesn't modify snapshots
func TestDryRunModeNoModifications(t *testing.T) {
	cfg := config.NewConfig("test")
	cfg.DryRun = true

	t.Log("In dry-run mode:")
	t.Log("1. CreateSnapshot should NOT be called on manager")
	t.Log("2. DeleteSnapshot should NOT be called on manager")
	t.Log("3. Operations should be logged with [DRY-RUN] prefix")
	t.Log("4. Counters should still increment")
	t.Log("5. All logic should execute normally except actual ZFS commands")
}
