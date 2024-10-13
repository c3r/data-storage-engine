package storage

import (
	"log"
	"slices"
	"time"

	"github.com/c3r/data-storage-engine/memtable"
	"github.com/c3r/data-storage-engine/segment"
)

var logger log.Logger = *log.Default()

type Storage struct {
	memory   *memtable.Memtable
	segments *segment.Segments
}

func New(maxSegmentSize int64, maxNumbersOfSegments int64, numberOfSegmentThreads int, segmentsDirPath string, compactionSleepSeconds int) *Storage {
	memory := memtable.New(maxSegmentSize, maxNumbersOfSegments)
	storage := &Storage{memory, segment.New()}
	for threadIdx := 0; threadIdx < numberOfSegmentThreads; threadIdx++ {
		go storage.segmentThread(segmentsDirPath)
	}
	go storage.compactThread(segmentsDirPath, compactionSleepSeconds)
	// Load segments from disk
	// storage.segments.LoadSegmentsFromDisk(segmentsDirPath)
	return storage
}

func (storage *Storage) compactThread(segmentsDirPath string, sleepTime int) {
	for {
		time.Sleep(time.Duration(sleepTime) * time.Second)
		segmentResponse := storage.segments.CompactSegmentData(&segment.SegmentCompactionRequest{
			DirPath: segmentsDirPath,
		})
		if segmentResponse.Error != nil {
			logger.Printf("Something went wrong when compacting rows: %s", segmentResponse.Error.Error())
			continue
		}
		logger.Printf("Compaction: %s.", segmentResponse.Message)
	}
}

func (storage *Storage) segmentThread(segmentsDirPath string) {
	for {
		id, rows := storage.memory.Dump()
		order := []string{}
		rows.Range(func(key, value string) bool {
			order = append(order, key)
			return true
		})
		slices.Sort(order)
		segmentResponse := storage.segments.PersistToFile(&segment.SegmentPersistToFileRequest{
			Id:      id,
			Order:   order,
			Rows:    rows,
			DirPath: segmentsDirPath,
		})
		logger.Printf("Segments: %s", segmentResponse.Message)
		if segmentResponse.Error != nil {
			panic(segmentResponse.Error)
		}
		storage.memory.Clear(id)
	}
}

func (storage *Storage) Save(key string, value string) {
	storage.memory.Store(key, value)
}

func (storage *Storage) Load(key string) (string, error, bool) {
	if value, ok := storage.memory.Load(key); ok {
		return value, nil, true
	}
	segmentResponse := storage.segments.LoadValue(key)
	return segmentResponse.Value, segmentResponse.Error, false
}
