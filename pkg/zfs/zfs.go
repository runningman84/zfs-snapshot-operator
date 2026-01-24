package zfs

import (
	"fmt"
	"log"
	"os/exec"
	"time"

	"github.com/runningman84/zfs-snapshot-operator/pkg/config"
	"github.com/runningman84/zfs-snapshot-operator/pkg/models"
	"github.com/runningman84/zfs-snapshot-operator/pkg/parser"
)

// Manager handles ZFS operations
type Manager struct {
	config *config.Config
}

// NewManager creates a new ZFS manager
func NewManager(cfg *config.Config) *Manager {
	return &Manager{
		config: cfg,
	}
}

// GetPools retrieves all ZFS pools
func (m *Manager) GetPools() ([]*models.Pool, error) {
	cmd := exec.Command(m.config.ZFSListPoolsCmd[0], m.config.ZFSListPoolsCmd[1:]...)
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("command failed: %w", err)
	}

	pools, err := parser.ParsePoolsJSON(output)
	if err != nil {
		return nil, fmt.Errorf("failed to parse pools JSON: %w", err)
	}

	return pools, nil
}

// GetSnapshots retrieves snapshots for a pool/filesystem
func (m *Manager) GetSnapshots(poolName, filesystemName, frequency string) ([]*models.Snapshot, error) {
	cmd := exec.Command(m.config.ZFSListSnapshotsCmd[0], m.config.ZFSListSnapshotsCmd[1:]...)
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("command failed: %w", err)
	}

	allSnapshots, err := parser.ParseSnapshotsJSON(output)
	if err != nil {
		return nil, fmt.Errorf("failed to parse snapshots JSON: %w", err)
	}

	// Filter snapshots if frequency is specified
	var snapshots []*models.Snapshot
	for _, snapshot := range allSnapshots {
		if frequency != "" && snapshot.Frequency != frequency {
			continue
		}
		snapshots = append(snapshots, snapshot)
	}

	return snapshots, nil
}

// DeleteSnapshot deletes a ZFS snapshot
func (m *Manager) DeleteSnapshot(snapshot *models.Snapshot) error {
	log.Printf("Deleting snapshot %s", snapshot.SnapshotName)

	snapshotPath := fmt.Sprintf("%s/%s@%s", snapshot.PoolName, snapshot.FilesystemName, snapshot.SnapshotName)

	var cmd *exec.Cmd
	if m.config.TestMode {
		cmd = exec.Command(m.config.ZFSDeleteSnapshotCmd[0], m.config.ZFSDeleteSnapshotCmd[1:]...)
	} else {
		args := append(m.config.ZFSDeleteSnapshotCmd, snapshotPath)
		cmd = exec.Command(args[0], args[1:]...)
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("command failed: %w, output: %s", err, string(output))
	}

	return nil
}

// CreateSnapshot creates a new ZFS snapshot
func (m *Manager) CreateSnapshot(snapshot *models.Snapshot) error {
	log.Printf("Creating snapshot %s", snapshot.SnapshotName)

	snapshotPath := fmt.Sprintf("%s/%s@%s", snapshot.PoolName, snapshot.FilesystemName, snapshot.SnapshotName)

	var cmd *exec.Cmd
	if m.config.TestMode {
		cmd = exec.Command(m.config.ZFSCreateSnapshotCmd[0], m.config.ZFSCreateSnapshotCmd[1:]...)
	} else {
		args := append(m.config.ZFSCreateSnapshotCmd, snapshotPath)
		cmd = exec.Command(args[0], args[1:]...)
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("command failed: %w, output: %s", err, string(output))
	}

	return nil
}

// IsSnapshotRecent checks if a snapshot is recent enough for the given frequency
func (m *Manager) IsSnapshotRecent(snapshot *models.Snapshot, frequency string, now time.Time) bool {
	if snapshot.Frequency == "" || snapshot.Frequency != frequency {
		return false
	}

	minDate := m.config.GetMinSnapshotDate(frequency, now)
	return snapshot.DateTime.After(minDate)
}

// CanSnapshotBeDeleted checks if a snapshot can be deleted based on frequency and age
func (m *Manager) CanSnapshotBeDeleted(snapshot *models.Snapshot, frequency string, now time.Time) bool {
	if snapshot.Frequency == "" || snapshot.Frequency != frequency {
		return false
	}

	maxDate := m.config.GetMaxSnapshotDate(frequency, now)
	return snapshot.DateTime.Before(maxDate)
}

// GetPoolStatus retrieves the status of all ZFS pools
func (m *Manager) GetPoolStatus() (map[string]*models.PoolStatus, error) {
	cmd := exec.Command(m.config.ZPoolStatusCmd[0], m.config.ZPoolStatusCmd[1:]...)
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("command failed: %w", err)
	}

	status, err := parser.ParsePoolStatusJSON(output)
	if err != nil {
		return nil, fmt.Errorf("failed to parse pool status JSON: %w", err)
	}

	return status, nil
}

// IsPoolHealthy checks if a pool is healthy and safe for operations
func (m *Manager) IsPoolHealthy(poolName string, poolStatus map[string]*models.PoolStatus) bool {
	status, exists := poolStatus[poolName]
	if !exists {
		log.Printf("Warning: No status found for pool %s", poolName)
		return false
	}

	// Pool should be ONLINE and have no errors
	if status.State != "ONLINE" {
		log.Printf("Pool %s is not ONLINE (state: %s)", poolName, status.State)
		return false
	}

	// Check error count (should be "0" for healthy pools)
	if status.ErrorCount != "0" && status.ErrorCount != "" {
		log.Printf("Pool %s has %s errors", poolName, status.ErrorCount)
		return false
	}

	return true
}
