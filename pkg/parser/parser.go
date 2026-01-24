package parser

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/runningman84/zfs-snapshot-operator/pkg/models"
)

// ZFSSnapshotJSON represents a ZFS snapshot in JSON format
type ZFSSnapshotJSON struct {
	Name         string                 `json:"name"`
	Type         string                 `json:"type"`
	Pool         string                 `json:"pool"`
	Dataset      string                 `json:"dataset"`
	SnapshotName string                 `json:"snapshot_name"`
	Properties   map[string]ZFSProperty `json:"properties,omitempty"`
}

// ZFSProperty represents a ZFS property value
type ZFSProperty struct {
	Value string `json:"value"`
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

		// Extract datetime from snapshot name (format: autosnap_2024-01-15_10:00:00_frequency)
		dateTime := time.Time{}
		datePattern := regexp.MustCompile(`(\d{4}-\d{2}-\d{2}_\d{2}:\d{2}:\d{2})`)
		dateMatches := datePattern.FindStringSubmatch(dataset.SnapshotName)
		if len(dateMatches) > 1 {
			parsedTime, err := time.Parse("2006-01-02_15:04:05", dateMatches[1])
			if err == nil {
				dateTime = parsedTime
			}
		}

		snapshots = append(snapshots, &models.Snapshot{
			PoolName:       dataset.Pool,
			FilesystemName: dataset.Dataset,
			SnapshotName:   dataset.SnapshotName,
			Frequency:      frequency,
			DateTime:       dateTime,
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

		// Extract used and available from properties
		used := ""
		avail := ""
		mountpoint := ""
		if dataset.Properties != nil {
			if usedProp, ok := dataset.Properties["used"]; ok {
				used = usedProp.Value
			}
			if availProp, ok := dataset.Properties["available"]; ok {
				avail = availProp.Value
			}
			if mountpointProp, ok := dataset.Properties["mountpoint"]; ok {
				mountpoint = mountpointProp.Value
			}
		}

		pools = append(pools, &models.Pool{
			PoolName:       poolName,
			FilesystemName: filesystemName,
			Used:           used,
			Avail:          avail,
			Mountpoint:     mountpoint,
		})
	}

	return pools, nil
}

// ZPoolStatusVdevJSON represents a vdev in the pool
type ZPoolStatusVdevJSON struct {
	Name           string                         `json:"name"`
	VdevType       string                         `json:"vdev_type"`
	State          string                         `json:"state"`
	AllocSpace     string                         `json:"alloc_space,omitempty"`
	TotalSpace     string                         `json:"total_space,omitempty"`
	ReadErrors     string                         `json:"read_errors,omitempty"`
	WriteErrors    string                         `json:"write_errors,omitempty"`
	ChecksumErrors string                         `json:"checksum_errors,omitempty"`
	Vdevs          map[string]ZPoolStatusVdevJSON `json:"vdevs,omitempty"`
}

// ZPoolStatusJSON represents zpool status in JSON format
type ZPoolStatusJSON struct {
	Name       string                         `json:"name"`
	State      string                         `json:"state"`
	Status     string                         `json:"status"`
	Action     string                         `json:"action"`
	ErrorCount string                         `json:"error_count"`
	Scan       *ZPoolStatusScanJSON           `json:"scan,omitempty"`
	ScanStats  *ZPoolStatusScanJSON           `json:"scan_stats,omitempty"` // Real zpool uses scan_stats
	Vdevs      map[string]ZPoolStatusVdevJSON `json:"vdevs,omitempty"`
}

// ZPoolStatusScanJSON represents the scan/scrub information
type ZPoolStatusScanJSON struct {
	Function  string      `json:"function"`   // "scrub"/"SCRUB" or "resilver"/"RESILVER"
	State     string      `json:"state"`      // "finished"/"FINISHED", "in_progress", etc.
	StartTime interface{} `json:"start_time"` // Can be int64 or string
	EndTime   interface{} `json:"end_time"`   // Can be int64 or string
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

		// Parse vdev information (space usage and errors)
		if rootVdev, ok := pool.Vdevs[poolName]; ok {
			ps.AllocSpace = rootVdev.AllocSpace
			ps.TotalSpace = rootVdev.TotalSpace
			ps.ReadErrors = rootVdev.ReadErrors
			ps.WriteErrors = rootVdev.WriteErrors
			ps.ChecksumErrors = rootVdev.ChecksumErrors
		}

		// Parse scrub information - check both scan and scan_stats fields
		scanInfo := pool.Scan
		if scanInfo == nil {
			scanInfo = pool.ScanStats
		}

		if scanInfo != nil {
			ps.ScrubFunction = strings.ToLower(scanInfo.Function)
			ps.ScrubState = strings.ToLower(scanInfo.State)

			// Parse end time - can be int64 or string timestamp
			if scanInfo.EndTime != nil {
				switch v := scanInfo.EndTime.(type) {
				case float64:
					ps.LastScrubTime = int64(v)
				case string:
					// Parse string timestamp like "Sat Jan 24 17:52:19 2026"
					if t, err := time.Parse("Mon Jan 2 15:04:05 2006", v); err == nil {
						ps.LastScrubTime = t.Unix()
					}
				}
			}

			// If end time is not set or zero, try start time
			if ps.LastScrubTime == 0 && scanInfo.StartTime != nil {
				switch v := scanInfo.StartTime.(type) {
				case float64:
					ps.LastScrubTime = int64(v)
				case string:
					if t, err := time.Parse("Mon Jan 2 15:04:05 2006", v); err == nil {
						ps.LastScrubTime = t.Unix()
					}
				}
			}
		}

		// Set default state if no scan info
		if scanInfo == nil {
			ps.ScrubState = "none"
		}

		statusMap[poolName] = ps
	}

	return statusMap, nil
}
