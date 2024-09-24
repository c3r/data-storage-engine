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

func New(maxSegmentSize int64, maxSegments int64, segmentThreads int) *Storage {
	memory := memtable.New(maxSegmentSize, maxSegments)
	logger.Printf("Creating storage of max segment size %d...", maxSegmentSize)
	storage := &Storage{memory, segment.SegmentTable{}}
	for i := 0; i < segmentThreads; i++ {
		go storage.segmentThread()
	}
	return storage
}

func (storage *Storage) segmentThread() {
	for {
		id, rows := storage.memory.Dump()
		order := []string{}
		rows.Range(func(key, value any) bool {
			order = append(order, key.(string))
			return true
		})
		slices.Sort(order)
		err := storage.segments.Create(order, rows)
		if err != nil {
			panic(err)
		}
		storage.memory.Clear(id)
	}
}

func (storage *Storage) Save(key string, value string) {
	storage.memory.Store(key, value)
}

func (storage *Storage) Load(key string) (string, error, bool) {
	fromMemory := true
	value, valueExists := storage.memory.Load(key)
	if !valueExists {
		fromMemory = false
		var err error
		value, err = storage.segments.Load(key)
		if err != nil {
			return "", err, false
		}
	}
	return value, nil, fromMemory
}
