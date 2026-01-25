package zfs

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"time"

	"github.com/runningman84/zfs-snapshot-operator/pkg/config"
	"github.com/runningman84/zfs-snapshot-operator/pkg/models"
	"github.com/runningman84/zfs-snapshot-operator/pkg/parser"
	"k8s.io/klog/v2"
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

// logCommand logs the command being executed if debug mode is enabled
func (m *Manager) logCommand(cmdArgs []string) {
	if m.config.IsDebug() {
		klog.V(1).Infof(" Executing command: %v", cmdArgs)
	}
}

// logCommandResult logs the command result if debug mode is enabled
func (m *Manager) logCommandResult(exitCode int, stdout, stderr []byte) {
	if m.config.IsDebug() {
		klog.V(1).Infof(" Exit code: %d", exitCode)
		if len(stdout) > 0 {
			klog.V(1).Infof(" stdout: %s", string(stdout))
		}
		if len(stderr) > 0 {
			klog.V(1).Infof(" stderr: %s", string(stderr))
		}
	}
}

// VersionInfo holds ZFS version information
type VersionInfo struct {
	Userland string `json:"userland"`
	Kernel   string `json:"kernel"`
}

// VersionOutput is the complete version JSON output
type VersionOutput struct {
	ZFSVersion VersionInfo `json:"zfs_version"`
}

// GetVersion retrieves ZFS userland and kernel versions
func (m *Manager) GetVersion() (string, string, error) {
	m.logCommand(m.config.ZFSVersionCmd)
	cmd := exec.Command(m.config.ZFSVersionCmd[0], m.config.ZFSVersionCmd[1:]...)
	output, err := cmd.CombinedOutput()
	exitCode := 0
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			exitCode = exitError.ExitCode()
		}
		m.logCommandResult(exitCode, output, nil)
		return "", "", fmt.Errorf("zfs version command failed: %w", err)
	}
	m.logCommandResult(0, output, nil)

	var versionOutput VersionOutput
	if err := json.Unmarshal(output, &versionOutput); err != nil {
		return "", "", fmt.Errorf("failed to parse version JSON: %w", err)
	}

	return versionOutput.ZFSVersion.Userland, versionOutput.ZFSVersion.Kernel, nil
}

// GetPools retrieves all ZFS pools
func (m *Manager) GetPools() ([]*models.Pool, error) {
	m.logCommand(m.config.ZFSListPoolsCmd)
	cmd := exec.Command(m.config.ZFSListPoolsCmd[0], m.config.ZFSListPoolsCmd[1:]...)
	output, err := cmd.CombinedOutput()
	exitCode := 0
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			exitCode = exitError.ExitCode()
		}
		m.logCommandResult(exitCode, output, nil)
		return nil, fmt.Errorf("command failed: %w", err)
	}
	m.logCommandResult(0, output, nil)

	pools, err := parser.ParsePoolsJSON(output)
	if err != nil {
		return nil, fmt.Errorf("failed to parse pools JSON: %w", err)
	}

	return pools, nil
}

// GetSnapshots retrieves snapshots for a pool/filesystem
func (m *Manager) GetSnapshots(poolName, filesystemName, frequency string) ([]*models.Snapshot, error) {
	m.logCommand(m.config.ZFSListSnapshotsCmd)
	cmd := exec.Command(m.config.ZFSListSnapshotsCmd[0], m.config.ZFSListSnapshotsCmd[1:]...)
	output, err := cmd.CombinedOutput()
	exitCode := 0
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			exitCode = exitError.ExitCode()
		}
		m.logCommandResult(exitCode, output, nil)
		return nil, fmt.Errorf("command failed: %w", err)
	}
	m.logCommandResult(0, output, nil)

	allSnapshots, err := parser.ParseSnapshotsJSON(output, m.config.SnapshotPrefix)
	if err != nil {
		return nil, fmt.Errorf("failed to parse snapshots JSON: %w", err)
	}

	// Filter snapshots by pool, filesystem, and frequency
	var snapshots []*models.Snapshot
	for _, snapshot := range allSnapshots {
		// Filter by pool name if specified
		if poolName != "" && snapshot.PoolName != poolName {
			continue
		}
		// Filter by filesystem name if specified
		if filesystemName != "" && snapshot.FilesystemName != filesystemName {
			continue
		}
		// Filter by frequency if specified
		if frequency != "" && snapshot.Frequency != frequency {
			continue
		}
		snapshots = append(snapshots, snapshot)
	}

	return snapshots, nil
}

// DeleteSnapshot deletes a ZFS snapshot
func (m *Manager) DeleteSnapshot(snapshot *models.Snapshot) error {
	klog.Infof("Deleting snapshot %s", snapshot.SnapshotName)

	// FilesystemName already includes the pool name (e.g., "usbstorage/private")
	snapshotPath := fmt.Sprintf("%s@%s", snapshot.FilesystemName, snapshot.SnapshotName)

	var cmd *exec.Cmd
	var cmdArgs []string
	if m.config.Mode == "test" {
		cmdArgs = m.config.ZFSDeleteSnapshotCmd
		cmd = exec.Command(cmdArgs[0], cmdArgs[1:]...)
	} else {
		cmdArgs = append(m.config.ZFSDeleteSnapshotCmd, snapshotPath)
		cmd = exec.Command(cmdArgs[0], cmdArgs[1:]...)
	}
	m.logCommand(cmdArgs)

	output, err := cmd.CombinedOutput()
	exitCode := 0
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			exitCode = exitError.ExitCode()
		}
		m.logCommandResult(exitCode, output, nil)
		return fmt.Errorf("command failed: %w, output: %s", err, string(output))
	}
	m.logCommandResult(0, output, nil)

	return nil
}

// CreateSnapshot creates a new ZFS snapshot
func (m *Manager) CreateSnapshot(snapshot *models.Snapshot) error {
	klog.Infof("Creating snapshot %s", snapshot.SnapshotName)

	// FilesystemName already includes the pool name (e.g., "usbstorage/private")
	snapshotPath := fmt.Sprintf("%s@%s", snapshot.FilesystemName, snapshot.SnapshotName)

	var cmd *exec.Cmd
	var cmdArgs []string
	if m.config.Mode == "test" {
		cmdArgs = m.config.ZFSCreateSnapshotCmd
		cmd = exec.Command(cmdArgs[0], cmdArgs[1:]...)
	} else {
		cmdArgs = append(m.config.ZFSCreateSnapshotCmd, snapshotPath)
		cmd = exec.Command(cmdArgs[0], cmdArgs[1:]...)
	}
	m.logCommand(cmdArgs)

	output, err := cmd.CombinedOutput()
	exitCode := 0
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			exitCode = exitError.ExitCode()
		}
		m.logCommandResult(exitCode, output, nil)
		return fmt.Errorf("command failed: %w, output: %s", err, string(output))
	}
	m.logCommandResult(0, output, nil)

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
	m.logCommand(m.config.ZPoolStatusCmd)
	cmd := exec.Command(m.config.ZPoolStatusCmd[0], m.config.ZPoolStatusCmd[1:]...)
	output, err := cmd.CombinedOutput()
	exitCode := 0
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			exitCode = exitError.ExitCode()
		}
		m.logCommandResult(exitCode, output, nil)
		return nil, fmt.Errorf("command failed: %w", err)
	}
	m.logCommandResult(0, output, nil)

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
		klog.Infof("Warning: No status found for pool %s", poolName)
		return false
	}

	// Pool should be ONLINE and have no errors
	if status.State != "ONLINE" {
		klog.Infof("Pool %s is not ONLINE (state: %s)", poolName, status.State)
		return false
	}

	// Check error count (should be "0" for healthy pools)
	if status.ErrorCount != "0" && status.ErrorCount != "" {
		klog.Infof("Pool %s has %s errors", poolName, status.ErrorCount)
		return false
	}

	return true
}
