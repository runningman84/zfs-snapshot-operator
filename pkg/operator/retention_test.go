package operator

import (
	"testing"
	"time"
)

// retention_test.go contains tests for the time-window retention logic with deduplication.
//
// The retention system works as follows:
// 1. Snapshots are grouped by time period (hour/day/week/month/year)
// 2. Within each period, only the newest snapshot is kept
// 3. Only periods within the retention window are retained
//
// For example, with maxYearly: 3:
// - Retention window: last 3 years
// - If there are multiple yearly snapshots in 2024, keep only the newest one
// - Delete all yearly snapshots older than 3 years
//
// This ensures temporal coverage (one snapshot per period) rather than
// just keeping the N most recent snapshots.

func TestGetTimePeriodKey(t *testing.T) {
	op := &Operator{}

	tests := []struct {
		name      string
		timestamp string
		frequency string
		expected  string
	}{
		// Hourly tests
		{
			name:      "hourly snapshot groups by hour",
			timestamp: "2026-01-25 14:30:45",
			frequency: "hourly",
			expected:  "2026-01-25 14",
		},
		{
			name:      "hourly different minute same hour",
			timestamp: "2026-01-25 14:59:59",
			frequency: "hourly",
			expected:  "2026-01-25 14",
		},
		{
			name:      "hourly next hour",
			timestamp: "2026-01-25 15:00:00",
			frequency: "hourly",
			expected:  "2026-01-25 15",
		},

		// Daily tests
		{
			name:      "daily snapshot groups by day",
			timestamp: "2026-01-25 14:30:45",
			frequency: "daily",
			expected:  "2026-01-25",
		},
		{
			name:      "daily different time same day",
			timestamp: "2026-01-25 23:59:59",
			frequency: "daily",
			expected:  "2026-01-25",
		},
		{
			name:      "daily next day",
			timestamp: "2026-01-26 00:00:00",
			frequency: "daily",
			expected:  "2026-01-26",
		},

		// Weekly tests
		{
			name:      "weekly snapshot groups by ISO week",
			timestamp: "2026-01-25 14:30:45", // Week 4 of 2026
			frequency: "weekly",
			expected:  "2026-W04",
		},
		{
			name:      "weekly different day same week",
			timestamp: "2026-01-26 10:00:00", // Week 5
			frequency: "weekly",
			expected:  "2026-W05",
		},
		{
			name:      "weekly next week",
			timestamp: "2026-02-02 10:00:00", // Week 6
			frequency: "weekly",
			expected:  "2026-W06",
		},

		// Monthly tests
		{
			name:      "monthly snapshot groups by month",
			timestamp: "2026-01-25 14:30:45",
			frequency: "monthly",
			expected:  "2026-01",
		},
		{
			name:      "monthly different day same month",
			timestamp: "2026-01-31 23:59:59",
			frequency: "monthly",
			expected:  "2026-01",
		},
		{
			name:      "monthly next month",
			timestamp: "2026-02-01 00:00:00",
			frequency: "monthly",
			expected:  "2026-02",
		},

		// Yearly tests
		{
			name:      "yearly snapshot groups by year",
			timestamp: "2026-01-25 14:30:45",
			frequency: "yearly",
			expected:  "2026",
		},
		{
			name:      "yearly different month same year",
			timestamp: "2026-12-31 23:59:59",
			frequency: "yearly",
			expected:  "2026",
		},
		{
			name:      "yearly next year",
			timestamp: "2027-01-01 00:00:00",
			frequency: "yearly",
			expected:  "2027",
		},
		{
			name:      "yearly previous year",
			timestamp: "2024-03-12 16:30:00",
			frequency: "yearly",
			expected:  "2024",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts, err := time.Parse("2006-01-02 15:04:05", tt.timestamp)
			if err != nil {
				t.Fatalf("Failed to parse timestamp: %v", err)
			}

			result := op.getTimePeriodKey(ts, tt.frequency)
			if result != tt.expected {
				t.Errorf("getTimePeriodKey(%s, %s) = %s, want %s",
					tt.timestamp, tt.frequency, result, tt.expected)
			}
		})
	}
}

func TestGetTimePeriodKeyConsistency(t *testing.T) {
	op := &Operator{}

	// Test that multiple snapshots on the same day have the same daily key
	day1_morning := time.Date(2026, 1, 25, 8, 0, 0, 0, time.UTC)
	day1_afternoon := time.Date(2026, 1, 25, 16, 30, 0, 0, time.UTC)
	day1_evening := time.Date(2026, 1, 25, 23, 59, 59, 0, time.UTC)

	key1 := op.getTimePeriodKey(day1_morning, "daily")
	key2 := op.getTimePeriodKey(day1_afternoon, "daily")
	key3 := op.getTimePeriodKey(day1_evening, "daily")

	if key1 != key2 || key2 != key3 {
		t.Errorf("Daily snapshots on same day should have same key: %s, %s, %s", key1, key2, key3)
	}

	// Test that snapshots on different days have different keys
	day2 := time.Date(2026, 1, 26, 12, 0, 0, 0, time.UTC)
	key4 := op.getTimePeriodKey(day2, "daily")

	if key1 == key4 {
		t.Errorf("Daily snapshots on different days should have different keys: %s vs %s", key1, key4)
	}
}

func TestYearlyDeduplication(t *testing.T) {
	op := &Operator{}

	// Simulate the scenario from the user's logs:
	// Multiple yearly snapshots in 2024, some in 2025, some in 2026
	snapshots := []struct {
		timestamp string
		year      string
	}{
		{"2024-01-01 00:00:02", "2024"},
		{"2024-03-12 14:15:01", "2024"},
		{"2024-03-12 16:30:00", "2024"},
		{"2024-03-12 17:15:01", "2024"},
		{"2025-01-01 00:00:00", "2025"},
		{"2026-01-25 12:00:00", "2026"},
	}

	// All 2024 snapshots should map to the same key
	var key2024 string
	for _, snap := range snapshots {
		ts, _ := time.Parse("2006-01-02 15:04:05", snap.timestamp)
		key := op.getTimePeriodKey(ts, "yearly")

		if snap.year == "2024" {
			if key2024 == "" {
				key2024 = key
			}
			if key != "2024" {
				t.Errorf("Snapshot %s should map to year 2024, got %s", snap.timestamp, key)
			}
			if key != key2024 {
				t.Errorf("All 2024 snapshots should have same key, got %s vs %s", key, key2024)
			}
		}
	}

	// Verify different years have different keys
	ts2024, _ := time.Parse("2006-01-02 15:04:05", "2024-03-12 16:30:00")
	ts2025, _ := time.Parse("2006-01-02 15:04:05", "2025-01-01 00:00:00")
	ts2026, _ := time.Parse("2006-01-02 15:04:05", "2026-01-25 12:00:00")

	key2024 = op.getTimePeriodKey(ts2024, "yearly")
	key2025 := op.getTimePeriodKey(ts2025, "yearly")
	key2026 := op.getTimePeriodKey(ts2026, "yearly")

	if key2024 == key2025 || key2025 == key2026 || key2024 == key2026 {
		t.Errorf("Different years should have different keys: 2024=%s, 2025=%s, 2026=%s",
			key2024, key2025, key2026)
	}
}

func TestWeeklyISOWeekGrouping(t *testing.T) {
	op := &Operator{}

	// Test ISO week boundaries
	// ISO week 1 of 2026 starts on Monday, December 29, 2025
	// and ends on Sunday, January 4, 2026

	tests := []struct {
		name     string
		date     string
		expected string
	}{
		{
			name:     "last day of ISO week 1",
			date:     "2026-01-04 23:59:59",
			expected: "2026-W01",
		},
		{
			name:     "first day of ISO week 2",
			date:     "2026-01-05 00:00:00",
			expected: "2026-W02",
		},
		{
			name:     "middle of ISO week 4",
			date:     "2026-01-25 12:00:00",
			expected: "2026-W04",
		},
		{
			name:     "end of ISO week 4",
			date:     "2026-02-01 23:59:59",
			expected: "2026-W05",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts, err := time.Parse("2006-01-02 15:04:05", tt.date)
			if err != nil {
				t.Fatalf("Failed to parse date: %v", err)
			}

			key := op.getTimePeriodKey(ts, "weekly")
			if key != tt.expected {
				t.Errorf("getTimePeriodKey(%s, weekly) = %s, want %s", tt.date, key, tt.expected)
			}
		})
	}
}

func TestMonthlyGrouping(t *testing.T) {
	op := &Operator{}

	// Test that snapshots throughout the month map to the same key
	snapshots := []string{
		"2026-01-01 00:00:00",
		"2026-01-15 12:30:00",
		"2026-01-31 23:59:59",
	}

	var keys []string
	for _, snap := range snapshots {
		ts, _ := time.Parse("2006-01-02 15:04:05", snap)
		key := op.getTimePeriodKey(ts, "monthly")
		keys = append(keys, key)
	}

	// All should be "2026-01"
	for i, key := range keys {
		if key != "2026-01" {
			t.Errorf("Snapshot %d (%s) mapped to %s, want 2026-01", i, snapshots[i], key)
		}
	}

	// Next month should be different
	nextMonth, _ := time.Parse("2006-01-02 15:04:05", "2026-02-01 00:00:00")
	nextKey := op.getTimePeriodKey(nextMonth, "monthly")
	if nextKey == keys[0] {
		t.Errorf("February should have different key than January, both are %s", nextKey)
	}
}

func TestEdgeCases(t *testing.T) {
	op := &Operator{}

	tests := []struct {
		name      string
		timestamp time.Time
		frequency string
		shouldRun bool
	}{
		{
			name:      "zero time value",
			timestamp: time.Time{},
			frequency: "yearly",
			shouldRun: true,
		},
		{
			name:      "leap year day",
			timestamp: time.Date(2024, 2, 29, 12, 0, 0, 0, time.UTC),
			frequency: "daily",
			shouldRun: true,
		},
		{
			name:      "year boundary",
			timestamp: time.Date(2025, 12, 31, 23, 59, 59, 999999999, time.UTC),
			frequency: "yearly",
			shouldRun: true,
		},
		{
			name:      "month boundary",
			timestamp: time.Date(2026, 1, 31, 23, 59, 59, 0, time.UTC),
			frequency: "monthly",
			shouldRun: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Should not panic
			key := op.getTimePeriodKey(tt.timestamp, tt.frequency)
			if key == "" {
				t.Errorf("Expected non-empty key for %s", tt.name)
			}
		})
	}
}

func TestInvalidFrequency(t *testing.T) {
	op := &Operator{}

	ts := time.Date(2026, 1, 25, 12, 0, 0, 0, time.UTC)

	// Test with invalid frequency - should fall back to full timestamp
	key := op.getTimePeriodKey(ts, "invalid")

	// Should return a timestamp string (the default case)
	expected := ts.Format("2006-01-02 15:04:05")
	if key != expected {
		t.Errorf("Invalid frequency should fall back to full timestamp: got %s, want %s", key, expected)
	}
}
