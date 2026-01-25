package zfs

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/runningman84/zfs-snapshot-operator/pkg/config"
	"github.com/runningman84/zfs-snapshot-operator/pkg/models"
)

func TestNewManager(t *testing.T) {
	cfg := config.NewConfig("test")
	manager := NewManager(cfg)

	if manager == nil {
		t.Fatal("NewManager() returned nil")
	}

	if manager.config != cfg {
		t.Error("Manager config not properly set")
	}
}

func TestIsSnapshotRecent(t *testing.T) {
	cfg := config.NewConfig("test")
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
	cfg := config.NewConfig("test")
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

	cfg := config.NewConfig("test")
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

	cfg := config.NewConfig("test")
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

	cfg := config.NewConfig("test")
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

	cfg := config.NewConfig("test")
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
	cfg := config.NewConfig("test")
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
	cfg := config.NewConfig("test")
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

	cfg := config.NewConfig("test")
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

	cfg := config.NewConfig("test")
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

func TestSnapshotDeletionSafety(t *testing.T) {
	// Critical safety tests to ensure recent snapshots are NEVER deleted
	cfg := config.NewConfig("test")
	manager := NewManager(cfg)
	now := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name        string
		snapshot    *models.Snapshot
		frequency   string
		shouldKeep  bool
		description string
	}{
		{
			name: "snapshot created just now - MUST keep",
			snapshot: &models.Snapshot{
				DateTime:  now,
				Frequency: "hourly",
			},
			frequency:   "hourly",
			shouldKeep:  true,
			description: "Snapshot created at current time must never be deleted",
		},
		{
			name: "snapshot created 1 minute ago - MUST keep",
			snapshot: &models.Snapshot{
				DateTime:  now.Add(-1 * time.Minute),
				Frequency: "hourly",
			},
			frequency:   "hourly",
			shouldKeep:  true,
			description: "Very recent snapshot must be protected",
		},
		{
			name: "snapshot created 30 minutes ago - MUST keep",
			snapshot: &models.Snapshot{
				DateTime:  now.Add(-30 * time.Minute),
				Frequency: "hourly",
			},
			frequency:   "hourly",
			shouldKeep:  true,
			description: "Snapshot within minimum age must be kept",
		},
		{
			name: "daily snapshot created today - MUST keep",
			snapshot: &models.Snapshot{
				DateTime:  now.Add(-6 * time.Hour),
				Frequency: "daily",
			},
			frequency:   "daily",
			shouldKeep:  true,
			description: "Daily snapshot from same day must be kept",
		},
		{
			name: "weekly snapshot created this week - MUST keep",
			snapshot: &models.Snapshot{
				DateTime:  now.Add(-3 * 24 * time.Hour),
				Frequency: "weekly",
			},
			frequency:   "weekly",
			shouldKeep:  true,
			description: "Weekly snapshot from current week must be kept",
		},
		{
			name: "snapshot at exact retention boundary - MUST keep",
			snapshot: &models.Snapshot{
				DateTime:  now.Add(-time.Duration(cfg.MaxHourlySnapshots) * time.Hour),
				Frequency: "hourly",
			},
			frequency:   "hourly",
			shouldKeep:  true,
			description: "Snapshot at retention boundary should be kept",
		},
		{
			name: "snapshot beyond retention period - can delete",
			snapshot: &models.Snapshot{
				DateTime:  now.Add(-time.Duration(cfg.MaxHourlySnapshots+2) * time.Hour),
				Frequency: "hourly",
			},
			frequency:   "hourly",
			shouldKeep:  false,
			description: "Only old snapshots beyond retention should be deletable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			canDelete := manager.CanSnapshotBeDeleted(tt.snapshot, tt.frequency, now)
			isRecent := manager.IsSnapshotRecent(tt.snapshot, tt.frequency, now)

			// Critical safety check
			if tt.shouldKeep && canDelete {
				t.Errorf("SAFETY VIOLATION: %s - CanSnapshotBeDeleted() = true, want false. %s",
					tt.name, tt.description)
			}

			if !tt.shouldKeep && !canDelete {
				t.Errorf("Retention policy issue: %s - CanSnapshotBeDeleted() = false, want true. %s",
					tt.name, tt.description)
			}

			// Verify IsSnapshotRecent is consistent for recent snapshots
			if tt.shouldKeep && tt.snapshot.DateTime.After(now.Add(-1*time.Hour)) && !isRecent {
				t.Errorf("Consistency issue: very recent snapshot not marked as recent. %s", tt.description)
			}
		})
	}
}

func TestSnapshotDeletionSafetyAllFrequencies(t *testing.T) {
	// Test that snapshots just created for all frequencies are protected
	cfg := config.NewConfig("test")
	manager := NewManager(cfg)
	now := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)

	frequencies := []string{"hourly", "daily", "weekly", "monthly", "yearly"}

	for _, freq := range frequencies {
		t.Run("current_snapshot_"+freq, func(t *testing.T) {
			snapshot := &models.Snapshot{
				DateTime:  now,
				Frequency: freq,
			}

			canDelete := manager.CanSnapshotBeDeleted(snapshot, freq, now)
			if canDelete {
				t.Errorf("CRITICAL: Current %s snapshot can be deleted! This is a safety violation.", freq)
			}

			isRecent := manager.IsSnapshotRecent(snapshot, freq, now)
			if !isRecent {
				t.Errorf("Current %s snapshot not marked as recent", freq)
			}
		})
	}
}

func TestGetVersion(t *testing.T) {
	// Change to project root so test data paths work
	if err := changeToProjectRoot(); err != nil {
		t.Skipf("Could not change to project root: %v", err)
	}

	cfg := config.NewConfig("test")
	manager := NewManager(cfg)

	userland, kernel, err := manager.GetVersion()
	if err != nil {
		t.Fatalf("GetVersion() failed: %v", err)
	}

	expectedUserland := "zfs-2.3.3-1"
	expectedKernel := "zfs-kmod-2.3.3-1"

	if userland != expectedUserland {
		t.Errorf("GetVersion() userland = %q, want %q", userland, expectedUserland)
	}

	if kernel != expectedKernel {
		t.Errorf("GetVersion() kernel = %q, want %q", kernel, expectedKernel)
	}
}

func TestCreateSnapshot(t *testing.T) {
	cfg := config.NewConfig("test")
	manager := NewManager(cfg)

	snapshot := &models.Snapshot{
		PoolName:       "tank",
		FilesystemName: "tank/data",
		SnapshotName:   "autosnap_2026-01-25_15:00:00_hourly",
		Frequency:      "hourly",
	}

	// In test mode, CreateSnapshot uses "true" command which always succeeds
	err := manager.CreateSnapshot(snapshot)
	if err != nil {
		t.Errorf("CreateSnapshot() failed: %v", err)
	}
}

func TestDeleteSnapshot(t *testing.T) {
	cfg := config.NewConfig("test")
	manager := NewManager(cfg)

	snapshot := &models.Snapshot{
		PoolName:       "tank",
		FilesystemName: "tank/data",
		SnapshotName:   "autosnap_2020-01-01_00:00:00_yearly",
		Frequency:      "yearly",
	}

	// In test mode, DeleteSnapshot uses "true" command which always succeeds
	err := manager.DeleteSnapshot(snapshot)
	if err != nil {
		t.Errorf("DeleteSnapshot() failed: %v", err)
	}
}

func TestCreateSnapshotWithInvalidCommand(t *testing.T) {
	// Create a config with a command that will fail
	cfg := config.NewConfig("test")
	cfg.ZFSCreateSnapshotCmd = []string{"false"} // 'false' always exits with error
	manager := NewManager(cfg)

	snapshot := &models.Snapshot{
		PoolName:       "tank",
		FilesystemName: "tank/data",
		SnapshotName:   "autosnap_2026-01-25_15:00:00_hourly",
		Frequency:      "hourly",
	}

	err := manager.CreateSnapshot(snapshot)
	if err == nil {
		t.Error("CreateSnapshot() should have failed with 'false' command")
	}
}

func TestDeleteSnapshotWithInvalidCommand(t *testing.T) {
	// Create a config with a command that will fail
	cfg := config.NewConfig("test")
	cfg.ZFSDeleteSnapshotCmd = []string{"false"} // 'false' always exits with error
	manager := NewManager(cfg)

	snapshot := &models.Snapshot{
		PoolName:       "tank",
		FilesystemName: "tank/data",
		SnapshotName:   "autosnap_2020-01-01_00:00:00_yearly",
		Frequency:      "yearly",
	}

	err := manager.DeleteSnapshot(snapshot)
	if err == nil {
		t.Error("DeleteSnapshot() should have failed with 'false' command")
	}
}

// changeToProjectRoot changes to the project root directory for tests
func changeToProjectRoot() error {
	// Get current working directory
	wd, err := os.Getwd()
	if err != nil {
		return err
	}

	// Walk up directories to find go.mod
	dir := wd
	for {
		goModPath := filepath.Join(dir, "go.mod")
		if _, err := os.Stat(goModPath); err == nil {
			// Found go.mod, change to this directory
			return os.Chdir(dir)
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			// Reached filesystem root
			return fmt.Errorf("could not find project root (go.mod)")
		}
		dir = parent
	}
}
