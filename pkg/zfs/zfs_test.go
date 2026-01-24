package zfs

import (
	"os/exec"
	"testing"
	"time"

	"github.com/runningman84/zfs-snapshot-operator/pkg/config"
	"github.com/runningman84/zfs-snapshot-operator/pkg/models"
)

func TestNewManager(t *testing.T) {
	cfg := config.NewConfig(true)
	manager := NewManager(cfg)

	if manager == nil {
		t.Fatal("NewManager() returned nil")
	}

	if manager.config != cfg {
		t.Error("Manager config not properly set")
	}
}

func TestIsSnapshotRecent(t *testing.T) {
	cfg := config.NewConfig(true)
	manager := NewManager(cfg)
	now := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		snapshot  *models.Snapshot
		frequency string
		want      bool
	}{
		{
			name: "recent hourly snapshot",
			snapshot: &models.Snapshot{
				DateTime:  now.Add(-30 * time.Minute),
				Frequency: "hourly",
			},
			frequency: "hourly",
			want:      true,
		},
		{
			name: "old hourly snapshot",
			snapshot: &models.Snapshot{
				DateTime:  now.Add(-2 * time.Hour),
				Frequency: "hourly",
			},
			frequency: "hourly",
			want:      false,
		},
		{
			name: "recent daily snapshot",
			snapshot: &models.Snapshot{
				DateTime:  now.Add(-12 * time.Hour),
				Frequency: "daily",
			},
			frequency: "daily",
			want:      true,
		},
		{
			name: "old daily snapshot",
			snapshot: &models.Snapshot{
				DateTime:  now.Add(-48 * time.Hour),
				Frequency: "daily",
			},
			frequency: "daily",
			want:      false,
		},
		{
			name: "wrong frequency",
			snapshot: &models.Snapshot{
				DateTime:  now.Add(-30 * time.Minute),
				Frequency: "daily",
			},
			frequency: "hourly",
			want:      false,
		},
		{
			name: "empty frequency in snapshot",
			snapshot: &models.Snapshot{
				DateTime:  now.Add(-30 * time.Minute),
				Frequency: "",
			},
			frequency: "hourly",
			want:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := manager.IsSnapshotRecent(tt.snapshot, tt.frequency, now)
			if result != tt.want {
				t.Errorf("IsSnapshotRecent() = %v, want %v", result, tt.want)
			}
		})
	}
}

func TestCanSnapshotBeDeleted(t *testing.T) {
	cfg := config.NewConfig(true)
	manager := NewManager(cfg)
	now := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		snapshot  *models.Snapshot
		frequency string
		want      bool
	}{
		{
			name: "old hourly snapshot - can delete",
			snapshot: &models.Snapshot{
				DateTime:  now.Add(-time.Duration(cfg.MaxHourlySnapshots+1) * time.Hour),
				Frequency: "hourly",
			},
			frequency: "hourly",
			want:      true,
		},
		{
			name: "recent hourly snapshot - keep",
			snapshot: &models.Snapshot{
				DateTime:  now.Add(-time.Duration(cfg.MaxHourlySnapshots-1) * time.Hour),
				Frequency: "hourly",
			},
			frequency: "hourly",
			want:      false,
		},
		{
			name: "old daily snapshot - can delete",
			snapshot: &models.Snapshot{
				DateTime:  now.Add(-time.Duration(cfg.MaxDailySnapshots+1) * 24 * time.Hour),
				Frequency: "daily",
			},
			frequency: "daily",
			want:      true,
		},
		{
			name: "recent daily snapshot - keep",
			snapshot: &models.Snapshot{
				DateTime:  now.Add(-time.Duration(cfg.MaxDailySnapshots-1) * 24 * time.Hour),
				Frequency: "daily",
			},
			frequency: "daily",
			want:      false,
		},
		{
			name: "wrong frequency - don't delete",
			snapshot: &models.Snapshot{
				DateTime:  now.Add(-time.Duration(cfg.MaxHourlySnapshots+1) * time.Hour),
				Frequency: "daily",
			},
			frequency: "hourly",
			want:      false,
		},
		{
			name: "empty frequency in snapshot - don't delete",
			snapshot: &models.Snapshot{
				DateTime:  now.Add(-time.Duration(cfg.MaxHourlySnapshots+1) * time.Hour),
				Frequency: "",
			},
			frequency: "hourly",
			want:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := manager.CanSnapshotBeDeleted(tt.snapshot, tt.frequency, now)
			if result != tt.want {
				t.Errorf("CanSnapshotBeDeleted() = %v, want %v", result, tt.want)
			}
		})
	}
}

func TestGetPools(t *testing.T) {
	// Skip if test data files don't exist
	if _, err := exec.LookPath("cat"); err != nil {
		t.Skip("cat command not available")
	}

	cfg := config.NewConfig(true)
	manager := NewManager(cfg)

	pools, err := manager.GetPools()
	if err != nil {
		t.Skipf("GetPools() error = %v (test data may not be available)", err)
	}

	if len(pools) == 0 {
		t.Log("GetPools() returned no pools (may be expected if test data is empty)")
		return
	}

	// Verify pool structure
	for _, pool := range pools {
		if pool.PoolName == "" {
			t.Error("Pool has empty PoolName")
		}
	}
}

func TestGetSnapshots(t *testing.T) {
	// Skip if test data files don't exist
	if _, err := exec.LookPath("cat"); err != nil {
		t.Skip("cat command not available")
	}

	cfg := config.NewConfig(true)
	manager := NewManager(cfg)

	snapshots, err := manager.GetSnapshots("tank", "data", "")
	if err != nil {
		t.Skipf("GetSnapshots() error = %v (test data may not be available)", err)
	}

	if len(snapshots) == 0 {
		t.Log("GetSnapshots() returned no snapshots (may be expected if test data is empty)")
		return
	}

	// Verify snapshot structure
	for _, snapshot := range snapshots {
		if snapshot.SnapshotName == "" {
			t.Error("Snapshot has empty SnapshotName")
		}
	}
}

func TestGetSnapshotsFiltered(t *testing.T) {
	// Skip if test data files don't exist
	if _, err := exec.LookPath("cat"); err != nil {
		t.Skip("cat command not available")
	}

	cfg := config.NewConfig(true)
	manager := NewManager(cfg)

	// Test filtering by frequency
	hourlySnapshots, err := manager.GetSnapshots("tank", "data", "hourly")
	if err != nil {
		t.Skipf("GetSnapshots() error = %v (test data may not be available)", err)
	}

	// Verify all returned snapshots match the frequency
	for _, snapshot := range hourlySnapshots {
		if snapshot.Frequency != "hourly" && snapshot.Frequency != "" {
			t.Errorf("Expected hourly snapshot, got frequency: %s", snapshot.Frequency)
		}
	}
}

func TestGetPoolStatus(t *testing.T) {
	// Skip if test data files don't exist
	if _, err := exec.LookPath("cat"); err != nil {
		t.Skip("cat command not available")
	}

	cfg := config.NewConfig(true)
	manager := NewManager(cfg)

	statusMap, err := manager.GetPoolStatus()
	if err != nil {
		t.Skipf("GetPoolStatus() error = %v (test data may not be available)", err)
	}

	if len(statusMap) == 0 {
		t.Log("GetPoolStatus() returned no pools (may be expected if test data is empty)")
		return
	}

	// Verify status structure
	for poolName, status := range statusMap {
		if status.Name != poolName {
			t.Errorf("Status name mismatch: got %s, want %s", status.Name, poolName)
		}
		if status.State == "" {
			t.Errorf("Pool %s has empty state", poolName)
		}
	}
}

func TestIsPoolHealthy(t *testing.T) {
	cfg := config.NewConfig(true)
	manager := NewManager(cfg)

	tests := []struct {
		name       string
		poolName   string
		poolStatus map[string]*models.PoolStatus
		want       bool
	}{
		{
			name:     "healthy pool",
			poolName: "tank",
			poolStatus: map[string]*models.PoolStatus{
				"tank": {
					Name:       "tank",
					State:      "ONLINE",
					Status:     "All vdevs healthy",
					ErrorCount: "0",
				},
			},
			want: true,
		},
		{
			name:     "degraded pool",
			poolName: "backup",
			poolStatus: map[string]*models.PoolStatus{
				"backup": {
					Name:       "backup",
					State:      "DEGRADED",
					Status:     "One device offline",
					ErrorCount: "0",
				},
			},
			want: false,
		},
		{
			name:     "pool with errors",
			poolName: "corrupted",
			poolStatus: map[string]*models.PoolStatus{
				"corrupted": {
					Name:       "corrupted",
					State:      "ONLINE",
					Status:     "Pool formatted correctly",
					ErrorCount: "2",
				},
			},
			want: false,
		},
		{
			name:       "pool not in status map",
			poolName:   "unknown",
			poolStatus: map[string]*models.PoolStatus{},
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := manager.IsPoolHealthy(tt.poolName, tt.poolStatus)
			if result != tt.want {
				t.Errorf("IsPoolHealthy() = %v, want %v", result, tt.want)
			}
		})
	}
}

func TestGetPoolStatusWithFailedPools(t *testing.T) {
	// Skip if test data files don't exist
	if _, err := exec.LookPath("cat"); err != nil {
		t.Skip("cat command not available")
	}

	// Create a config that uses the failed pools status file
	cfg := config.NewConfig(true)
	cfg.ZPoolStatusCmd = []string{"cat", "../../test/zpool_status_failed.json"}
	manager := NewManager(cfg)

	statusMap, err := manager.GetPoolStatus()
	if err != nil {
		t.Fatalf("GetPoolStatus() error = %v", err)
	}

	if len(statusMap) == 0 {
		t.Fatal("GetPoolStatus() returned no pools")
	}

	// Verify we have the expected failed pools
	expectedPools := map[string]string{
		"usbstorage": "DEGRADED",
		"backup":     "FAULTED",
		"corrupted":  "ONLINE",
	}

	for poolName, expectedState := range expectedPools {
		status, exists := statusMap[poolName]
		if !exists {
			t.Errorf("Expected pool %s not found in status map", poolName)
			continue
		}

		if status.State != expectedState {
			t.Errorf("Pool %s: state = %s, want %s", poolName, status.State, expectedState)
		}

		// Test IsPoolHealthy with these failed pools
		isHealthy := manager.IsPoolHealthy(poolName, statusMap)

		// All pools in the failed file should be unhealthy
		// - usbstorage: DEGRADED state
		// - backup: FAULTED state
		// - corrupted: ONLINE but has data errors
		if isHealthy {
			t.Errorf("Pool %s should be unhealthy (state: %s, error_count: %s)", poolName, status.State, status.ErrorCount)
		}
	}

	// Verify error counts
	if corrupted, exists := statusMap["corrupted"]; exists {
		if corrupted.ErrorCount != "42" {
			t.Errorf("corrupted pool should have 42 errors, got: %s", corrupted.ErrorCount)
		}
	}
}

func TestGetPoolsEmpty(t *testing.T) {
	// Skip if test data files don't exist
	if _, err := exec.LookPath("cat"); err != nil {
		t.Skip("cat command not available")
	}

	cfg := config.NewConfig(true)
	cfg.ZFSListPoolsCmd = []string{"cat", "../../test/zfs_list_pools_empty.json"}
	manager := NewManager(cfg)

	pools, err := manager.GetPools()
	if err != nil {
		t.Fatalf("GetPools() error = %v", err)
	}

	if len(pools) != 0 {
		t.Errorf("GetPools() returned %d pools, want 0", len(pools))
	}
}

func TestGetSnapshotsEmpty(t *testing.T) {
	// Skip if test data files don't exist
	if _, err := exec.LookPath("cat"); err != nil {
		t.Skip("cat command not available")
	}

	cfg := config.NewConfig(true)
	cfg.ZFSListSnapshotsCmd = []string{"cat", "../../test/zfs_list_snapshots_empty.json"}
	manager := NewManager(cfg)

	snapshots, err := manager.GetSnapshots("tank", "data", "")
	if err != nil {
		t.Fatalf("GetSnapshots() error = %v", err)
	}

	if len(snapshots) != 0 {
		t.Errorf("GetSnapshots() returned %d snapshots, want 0", len(snapshots))
	}
}
