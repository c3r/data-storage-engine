package segment

import (
	"time"

	"github.com/c3r/data-storage-engine/syncmap"
)

type SegmentCompactionResponse struct {
	Ok       bool
	Duration time.Duration
	Message  string
	Error    error
}

type SegmentCompactionRequest struct {
	DirPath string
}

type SegmentLoadValueResponse struct {
	Ok      bool
	Value   string
	Message string
	Error   error
}

type SegmentLoadSegmentsFromDiskRequest struct {
	DirPath string
}

type SegmentLoadSegmentsFromDiskResponse struct {
	Ok      bool
	Message string
	Error   error
}

type SegmentPersistToFileRequest struct {
	Id      int64
	Order   []string
	Rows    *syncmap.SynchronizedMap[string, string]
	DirPath string
}

type SegmentPersistToFileResponse struct {
	Ok      bool
	Message string
	Error   error
}
