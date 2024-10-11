package storage

import (
	"log"
	"slices"

	"github.com/c3r/data-storage-engine/memtable"
	"github.com/c3r/data-storage-engine/segment"
)

var logger log.Logger = *log.Default()

type Storage struct {
	memory   *memtable.MemtableManager
	segments segment.SegmentTable
}

func New(maxSegmentSize int64, maxNumbersOfSegments int64, numberOfSegmentThreads int, segmentsDirPath string) *Storage {
	memory := memtable.New(maxSegmentSize, maxNumbersOfSegments)
	storage := &Storage{memory, segment.SegmentTable{}}
	// Init segment threads
	logger.Printf("Initializing %d segment threads...", numberOfSegmentThreads)
	for threadIdx := 0; threadIdx < numberOfSegmentThreads; threadIdx++ {
		go storage.segmentThread(segmentsDirPath)
	}
	// Load segments from disk
	storage.segments.LoadSegmentsFromDisk(segmentsDirPath)
	return storage
}

func (storage *Storage) segmentThread(segmentsDirPath string) {
	for {
		id, data := storage.memory.Dump()
		logger.Printf("Creating file for id %d", id)
		dataOrderIndex := []string{}
		data.Range(func(key, value string) bool {
			dataOrderIndex = append(dataOrderIndex, key)
			return true
		})
		slices.Sort(dataOrderIndex)
		filePath, err := storage.segments.CreatePersistentSegment(dataOrderIndex, data, segmentsDirPath)
		if err != nil {
			panic(err)
		}
		storage.memory.Clear(id)
		logger.Printf("File %s saved.", filePath)
	}
}

func (storage *Storage) Compact(segmentsDirPath string) {
	storage.segments.Compact(segmentsDirPath)
}

func (storage *Storage) Save(key string, value string) {
	storage.memory.Store(key, value)
}

func (storage *Storage) Load(key string) (string, error, bool) {
	if value, ok := storage.memory.Load(key); ok {
		return value, nil, true
	}
	value, err := storage.segments.Load(key)
	return value, err, false
}
