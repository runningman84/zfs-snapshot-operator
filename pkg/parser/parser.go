package parser

import (
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/runningman84/zfs-snapshot-operator/pkg/models"
)

// ZFSSnapshotJSON represents a ZFS snapshot in JSON format
type ZFSSnapshotJSON struct {
	Name         string `json:"name"`
	Type         string `json:"type"`
	Pool         string `json:"pool"`
	Dataset      string `json:"dataset"`
	SnapshotName string `json:"snapshot_name"`
}

// ZFSDatasetResponse represents the root response from zfs list -j
type ZFSDatasetResponse struct {
	OutputVersion struct {
		Command   string `json:"command"`
		VersMajor int    `json:"vers_major"`
		VersMinor int    `json:"vers_minor"`
	} `json:"output_version"`
	Datasets map[string]ZFSSnapshotJSON `json:"datasets"`
}

// ParseSnapshotsJSON parses zfs list snapshots JSON output
func ParseSnapshotsJSON(data []byte) ([]*models.Snapshot, error) {
	var response ZFSDatasetResponse

	if err := json.Unmarshal(data, &response); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	var snapshots []*models.Snapshot
	for _, dataset := range response.Datasets {
		if dataset.Type != "SNAPSHOT" {
			continue
		}

		// Extract frequency from snapshot name
		pattern := regexp.MustCompile(`.*_(yearly|monthly|weekly|daily|hourly|frequently)$`)
		matches := pattern.FindStringSubmatch(dataset.SnapshotName)

		frequency := ""
		if len(matches) > 1 {
			frequency = matches[1]
		}

		snapshots = append(snapshots, &models.Snapshot{
			PoolName:       dataset.Pool,
			FilesystemName: dataset.Dataset,
			SnapshotName:   dataset.SnapshotName,
			Frequency:      frequency,
		})
	}

	return snapshots, nil
}

// ParsePoolsJSON parses zfs list filesystems JSON output
func ParsePoolsJSON(data []byte) ([]*models.Pool, error) {
	var response ZFSDatasetResponse

	if err := json.Unmarshal(data, &response); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	var pools []*models.Pool
	for _, dataset := range response.Datasets {
		if dataset.Type != "FILESYSTEM" {
			continue
		}

		// Split the name to get pool and filesystem parts
		poolName := dataset.Pool
		filesystemName := ""

		// If the dataset name is different from pool name, extract filesystem
		if dataset.Name != dataset.Pool {
			filesystemName = dataset.Name
		}

		pools = append(pools, &models.Pool{
			PoolName:       poolName,
			FilesystemName: filesystemName,
		})
	}

	return pools, nil
}

// ZPoolStatusJSON represents zpool status in JSON format
type ZPoolStatusJSON struct {
	Name       string               `json:"name"`
	State      string               `json:"state"`
	Status     string               `json:"status"`
	Action     string               `json:"action"`
	ErrorCount string               `json:"error_count"`
	Scan       *ZPoolStatusScanJSON `json:"scan,omitempty"`
}

// ZPoolStatusScanJSON represents the scan/scrub information
type ZPoolStatusScanJSON struct {
	Function  string `json:"function"` // "scrub" or "resilver"
	State     string `json:"state"`    // "finished", "in_progress", etc.
	StartTime int64  `json:"start_time"`
	EndTime   int64  `json:"end_time"`
}

// ZPoolStatusResponse represents the root response from zpool status -j
type ZPoolStatusResponse struct {
	OutputVersion struct {
		Command   string `json:"command"`
		VersMajor int    `json:"vers_major"`
		VersMinor int    `json:"vers_minor"`
	} `json:"output_version"`
	Pools map[string]ZPoolStatusJSON `json:"pools"`
}

// ParsePoolStatusJSON parses zpool status JSON output
func ParsePoolStatusJSON(data []byte) (map[string]*models.PoolStatus, error) {
	var response ZPoolStatusResponse

	if err := json.Unmarshal(data, &response); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	statusMap := make(map[string]*models.PoolStatus)
	for poolName, pool := range response.Pools {
		ps := &models.PoolStatus{
			Name:       pool.Name,
			State:      pool.State,
			Status:     pool.Status,
			Action:     pool.Action,
			ErrorCount: pool.ErrorCount,
		}

		// Parse scrub information if available
		if pool.Scan != nil {
			ps.ScrubFunction = pool.Scan.Function
			ps.ScrubState = pool.Scan.State
			ps.LastScrubTime = pool.Scan.EndTime
			// If scrub is in progress, use start time
			if pool.Scan.State == "in_progress" || pool.Scan.EndTime == 0 {
				ps.LastScrubTime = pool.Scan.StartTime
			}
		} else {
			ps.ScrubState = "none"
		}

		statusMap[poolName] = ps
	}

	return statusMap, nil
}
