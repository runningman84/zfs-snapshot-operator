package models

import "time"

// Snapshot represents a ZFS snapshot
type Snapshot struct {
	PoolName       string
	FilesystemName string
	SnapshotName   string
	DateTime       time.Time
	Frequency      string
}

// Pool represents a ZFS pool/filesystem
type Pool struct {
	PoolName       string
	FilesystemName string
	Used           string
	Avail          string
	Mountpoint     string
}

// PoolStatus represents the health status of a ZFS pool
type PoolStatus struct {
	Name          string
	State         string
	Status        string
	Action        string
	ErrorCount    string
	LastScrubTime int64  // Unix timestamp of last scrub end time
	ScrubState    string // State of scrub: "finished", "in_progress", "none"
	ScrubFunction string // Function: "scrub" or "resilver"
}
