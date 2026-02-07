package zfs

import (
	"testing"
	"time"

	"github.com/runningman84/zfs-snapshot-operator/pkg/config"
	"github.com/runningman84/zfs-snapshot-operator/pkg/models"
)

// TestHourlySnapshotBoundaryBug demonstrates the bug fix for snapshots not being created
// when cronjobs run at slightly different times each hour
func TestHourlySnapshotBoundaryBug(t *testing.T) {
	cfg := config.NewConfig("test")
	manager := NewManager(cfg)

	// Scenario: Cronjob runs at different seconds each hour
	// Hour 1: Job runs at 12:00:30 - creates snapshot
	// Hour 2: Job runs at 13:00:05 - should create another snapshot (NEW HOUR!)

	hour1Run := time.Date(2024, 1, 15, 12, 0, 30, 0, time.UTC) // 12:00:30
	hour2Run := time.Date(2024, 1, 15, 13, 0, 5, 0, time.UTC)  // 13:00:05 (59m 35s later)

	// Snapshot from hour 1
	snapshotHour1 := &models.Snapshot{
		DateTime:  hour1Run,
		Frequency: "hourly",
	}

	// With the OLD buggy logic, this would return true (snapshot is < 1 hour old)
	// With the NEW fixed logic, this should return false (snapshot is from different hour period)
	isRecent := manager.IsSnapshotRecent(snapshotHour1, "hourly", hour2Run)

	if isRecent {
		t.Errorf("BUG: Snapshot from 12:00:30 is considered 'recent' at 13:00:05, " +
			"which would prevent creating a new snapshot for hour 13! " +
			"This causes missing hourly snapshots when cronjobs run at slightly different times.")
	}

	// Verify the period keys are different
	period1 := GetTimePeriodKey(hour1Run, "hourly")
	period2 := GetTimePeriodKey(hour2Run, "hourly")

	if period1 == period2 {
		t.Errorf("Period keys should be different: hour1=%s, hour2=%s", period1, period2)
	}

	// Expected: period1 = "2024-01-15 12", period2 = "2024-01-15 13"
	expectedPeriod1 := "2024-01-15 12"
	expectedPeriod2 := "2024-01-15 13"

	if period1 != expectedPeriod1 {
		t.Errorf("Hour 1 period = %s, want %s", period1, expectedPeriod1)
	}
	if period2 != expectedPeriod2 {
		t.Errorf("Hour 2 period = %s, want %s", period2, expectedPeriod2)
	}
}

// TestSnapshotWithinSameHour verifies that multiple snapshots within the same hour
// are correctly identified as being from the same period
func TestSnapshotWithinSameHour(t *testing.T) {
	cfg := config.NewConfig("test")
	manager := NewManager(cfg)

	// Both snapshots from hour 14 (14:00-14:59)
	snapshot1 := &models.Snapshot{
		DateTime:  time.Date(2024, 1, 15, 14, 5, 0, 0, time.UTC), // 14:05
		Frequency: "hourly",
	}

	now := time.Date(2024, 1, 15, 14, 55, 0, 0, time.UTC) // 14:55

	// Snapshot from earlier in the same hour should be considered recent
	isRecent := manager.IsSnapshotRecent(snapshot1, "hourly", now)

	if !isRecent {
		t.Errorf("Snapshot from 14:05 should be 'recent' at 14:55 (same hour)")
	}

	// Verify both are in the same period
	period1 := GetTimePeriodKey(snapshot1.DateTime, "hourly")
	period2 := GetTimePeriodKey(now, "hourly")

	if period1 != period2 {
		t.Errorf("Both timestamps should be in same hour: period1=%s, period2=%s", period1, period2)
	}
}
