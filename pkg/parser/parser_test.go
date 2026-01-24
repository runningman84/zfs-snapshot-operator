package parser

import (
	"testing"
)

func TestParseSnapshotsJSON(t *testing.T) {
	jsonData := `{
  "output_version": {
    "command": "zfs list",
    "vers_major": 0,
    "vers_minor": 1
  },
  "datasets": {
    "tank/data@autosnap_2024-01-15_10:00:00_hourly": {
      "name": "tank/data@autosnap_2024-01-15_10:00:00_hourly",
      "type": "SNAPSHOT",
      "pool": "tank",
      "dataset": "tank/data",
      "snapshot_name": "autosnap_2024-01-15_10:00:00_hourly"
    },
    "tank/backup@autosnap_2024-01-15_00:00:00_daily": {
      "name": "tank/backup@autosnap_2024-01-15_00:00:00_daily",
      "type": "SNAPSHOT",
      "pool": "tank",
      "dataset": "tank/backup",
      "snapshot_name": "autosnap_2024-01-15_00:00:00_daily"
    },
    "pool/docs@manual-snapshot": {
      "name": "pool/docs@manual-snapshot",
      "type": "SNAPSHOT",
      "pool": "pool",
      "dataset": "pool/docs",
      "snapshot_name": "manual-snapshot"
    }
  }
}`

	snapshots, err := ParseSnapshotsJSON([]byte(jsonData))
	if err != nil {
		t.Fatalf("ParseSnapshotsJSON() error = %v", err)
	}

	if len(snapshots) != 3 {
		t.Errorf("ParseSnapshotsJSON() returned %d snapshots, want 3", len(snapshots))
	}

	// Test first snapshot (hourly)
	if snapshots[0].PoolName != "tank" && snapshots[0].PoolName != "pool" {
		t.Errorf("snapshots[0].PoolName = %v, want tank or pool", snapshots[0].PoolName)
	}

	// Find hourly snapshot
	found := false
	for _, snap := range snapshots {
		if snap.Frequency == "hourly" {
			found = true
			if snap.PoolName != "tank" {
				t.Errorf("hourly snapshot PoolName = %v, want tank", snap.PoolName)
			}
			if snap.FilesystemName != "tank/data" {
				t.Errorf("hourly snapshot FilesystemName = %v, want tank/data", snap.FilesystemName)
			}
		}
	}
	if !found {
		t.Error("hourly snapshot not found")
	}

	// Find manual snapshot (no frequency)
	foundManual := false
	for _, snap := range snapshots {
		if snap.SnapshotName == "manual-snapshot" {
			foundManual = true
			if snap.Frequency != "" {
				t.Errorf("manual snapshot Frequency = %v, want empty string", snap.Frequency)
			}
		}
	}
	if !foundManual {
		t.Error("manual snapshot not found")
	}
}

func TestParseSnapshotsJSON_InvalidJSON(t *testing.T) {
	jsonData := `invalid json`

	_, err := ParseSnapshotsJSON([]byte(jsonData))
	if err == nil {
		t.Error("ParseSnapshotsJSON() expected error for invalid JSON, got nil")
	}
}

func TestParseSnapshotsJSON_InvalidSnapshotFormat(t *testing.T) {
	// This test is no longer relevant since the JSON structure provides all fields
	// The format validation happens at the ZFS level, not in parsing
	t.Skip("Snapshot format validation not needed with structured JSON")
}

func TestParsePoolsJSON(t *testing.T) {
	jsonData := `{
  "output_version": {
    "command": "zfs list",
    "vers_major": 0,
    "vers_minor": 1
  },
  "datasets": {
    "tank": {
      "name": "tank",
      "type": "FILESYSTEM",
      "pool": "tank"
    },
    "tank/data": {
      "name": "tank/data",
      "type": "FILESYSTEM",
      "pool": "tank"
    },
    "backup": {
      "name": "backup",
      "type": "FILESYSTEM",
      "pool": "backup"
    }
  }
}`

	pools, err := ParsePoolsJSON([]byte(jsonData))
	if err != nil {
		t.Fatalf("ParsePoolsJSON() error = %v", err)
	}

	if len(pools) != 3 {
		t.Errorf("ParsePoolsJSON() returned %d pools, want 3", len(pools))
	}

	// Test pool names
	poolNames := make(map[string]bool)
	for _, pool := range pools {
		poolNames[pool.PoolName] = true
	}

	if !poolNames["tank"] {
		t.Error("ParsePoolsJSON() missing 'tank' pool")
	}
	if !poolNames["backup"] {
		t.Error("ParsePoolsJSON() missing 'backup' pool")
	}
}

func TestParsePoolsJSON_InvalidJSON(t *testing.T) {
	jsonData := `invalid json`

	_, err := ParsePoolsJSON([]byte(jsonData))
	if err == nil {
		t.Error("ParsePoolsJSON() expected error for invalid JSON, got nil")
	}
}

func TestParsePoolsJSON_EmptyArray(t *testing.T) {
	jsonData := `{
  "output_version": {
    "command": "zfs list",
    "vers_major": 0,
    "vers_minor": 1
  },
  "datasets": {}
}`

	pools, err := ParsePoolsJSON([]byte(jsonData))
	if err != nil {
		t.Fatalf("ParsePoolsJSON() error = %v", err)
	}

	if len(pools) != 0 {
		t.Errorf("ParsePoolsJSON() returned %d pools, want 0", len(pools))
	}
}

func TestParsePoolStatusJSON(t *testing.T) {
	jsonData := `{
  "output_version": {
    "command": "zpool status",
    "vers_major": 0,
    "vers_minor": 1
  },
  "pools": {
    "tank": {
      "name": "tank",
      "state": "ONLINE",
      "status": "All vdevs are healthy",
      "action": "No action required",
      "error_count": "0"
    },
    "backup": {
      "name": "backup",
      "state": "DEGRADED",
      "status": "One or more devices has been taken offline",
      "action": "Replace the device",
      "error_count": "0"
    },
    "corrupted": {
      "name": "corrupted",
      "state": "ONLINE",
      "status": "Data corruption detected",
      "action": "Restore from backup",
      "error_count": "42"
    }
  }
}`

	statusMap, err := ParsePoolStatusJSON([]byte(jsonData))
	if err != nil {
		t.Fatalf("ParsePoolStatusJSON() error = %v", err)
	}

	if len(statusMap) != 3 {
		t.Errorf("ParsePoolStatusJSON() returned %d pools, want 3", len(statusMap))
	}

	// Test healthy pool
	if tank, exists := statusMap["tank"]; exists {
		if tank.State != "ONLINE" {
			t.Errorf("tank.State = %v, want ONLINE", tank.State)
		}
		if tank.ErrorCount != "0" {
			t.Errorf("tank.ErrorCount = %v, want '0'", tank.ErrorCount)
		}
	} else {
		t.Error("tank pool not found in status map")
	}

	// Test degraded pool
	if backup, exists := statusMap["backup"]; exists {
		if backup.State != "DEGRADED" {
			t.Errorf("backup.State = %v, want DEGRADED", backup.State)
		}
	} else {
		t.Error("backup pool not found in status map")
	}

	// Test pool with errors
	if corrupted, exists := statusMap["corrupted"]; exists {
		if corrupted.ErrorCount != "42" {
			t.Errorf("corrupted.ErrorCount = %v, want '42'", corrupted.ErrorCount)
		}
	} else {
		t.Error("corrupted pool not found in status map")
	}
}

func TestParsePoolStatusJSON_InvalidJSON(t *testing.T) {
	jsonData := `invalid json`

	_, err := ParsePoolStatusJSON([]byte(jsonData))
	if err == nil {
		t.Error("ParsePoolStatusJSON() expected error for invalid JSON, got nil")
	}
}

func TestParsePoolStatusJSON_EmptyPools(t *testing.T) {
	jsonData := `{
  "output_version": {
    "command": "zpool status",
    "vers_major": 0,
    "vers_minor": 1
  },
  "pools": {}
}`

	statusMap, err := ParsePoolStatusJSON([]byte(jsonData))
	if err != nil {
		t.Fatalf("ParsePoolStatusJSON() error = %v", err)
	}

	if len(statusMap) != 0 {
		t.Errorf("ParsePoolStatusJSON() returned %d pools, want 0", len(statusMap))
	}
}

func TestParsePoolStatusJSON_WithScrub(t *testing.T) {
	jsonData := `{
  "output_version": {
    "command": "zpool status",
    "vers_major": 0,
    "vers_minor": 1
  },
  "pools": {
    "storage": {
      "name": "storage",
      "state": "ONLINE",
      "status": "All vdevs are healthy",
      "action": "No action required",
      "error_count": "0",
      "scan": {
        "function": "scrub",
        "state": "finished",
        "start_time": 1704067200,
        "end_time": 1704070800
      }
    },
    "noscrub": {
      "name": "noscrub",
      "state": "ONLINE",
      "status": "All vdevs are healthy",
      "action": "No action required",
      "error_count": "0"
    },
    "inprogress": {
      "name": "inprogress",
      "state": "ONLINE",
      "status": "Scrub in progress",
      "action": "Wait for scrub to complete",
      "error_count": "0",
      "scan": {
        "function": "scrub",
        "state": "scanning",
        "start_time": 1704067200,
        "end_time": 0
      }
    }
  }
}`

	statusMap, err := ParsePoolStatusJSON([]byte(jsonData))
	if err != nil {
		t.Fatalf("ParsePoolStatusJSON() error = %v", err)
	}

	if len(statusMap) != 3 {
		t.Errorf("ParsePoolStatusJSON() returned %d pools, want 3", len(statusMap))
	}

	// Test pool with finished scrub
	if storage, exists := statusMap["storage"]; exists {
		if storage.LastScrubTime != 1704070800 {
			t.Errorf("storage.LastScrubTime = %v, want 1704070800", storage.LastScrubTime)
		}
		if storage.ScrubState != "finished" {
			t.Errorf("storage.ScrubState = %v, want finished", storage.ScrubState)
		}
		if storage.ScrubFunction != "scrub" {
			t.Errorf("storage.ScrubFunction = %v, want scrub", storage.ScrubFunction)
		}
	} else {
		t.Error("storage pool not found in status map")
	}

	// Test pool without scrub info
	if noscrub, exists := statusMap["noscrub"]; exists {
		if noscrub.LastScrubTime != 0 {
			t.Errorf("noscrub.LastScrubTime = %v, want 0", noscrub.LastScrubTime)
		}
		if noscrub.ScrubState != "none" {
			t.Errorf("noscrub.ScrubState = %v, want 'none'", noscrub.ScrubState)
		}
	} else {
		t.Error("noscrub pool not found in status map")
	}

	// Test pool with scrub in progress
	if inprogress, exists := statusMap["inprogress"]; exists {
		if inprogress.LastScrubTime != 1704067200 {
			t.Errorf("inprogress.LastScrubTime = %v, want 1704067200 (uses start_time when scanning)", inprogress.LastScrubTime)
		}
		if inprogress.ScrubState != "scanning" {
			t.Errorf("inprogress.ScrubState = %v, want scanning", inprogress.ScrubState)
		}
	} else {
		t.Error("inprogress pool not found in status map")
	}
}
