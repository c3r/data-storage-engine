package storage

import (
	"log"
	"slices"

	"github.com/c3r/data-storage-engine/memtable"
	"github.com/c3r/data-storage-engine/segment"
)

var logger log.Logger = *log.Default()

type Storage struct {
	memory    *memtable.MemtableManager
	segments  segment.SegmentTable
	dumpQueue chan int64
}

func New(maxSegmentSize int64, maxSegments int64) *Storage {
	memory := memtable.New(maxSegmentSize)
	logger.Printf("Creating storage of max segment size %d...", maxSegmentSize)
	storage := &Storage{
		memory:    memory,
		segments:  segment.SegmentTable{},
		dumpQueue: make(chan int64, maxSegments),
	}

	for i := 0; i < 5; i++ {
		go func(memory *memtable.MemtableManager) {
			logger.Println("Staring segment-creator thread...")
			for {
				id := <-storage.dumpQueue
				if rows, valueExists := memory.Dump(id); valueExists {
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
					logger.Printf("Segment-creator clearing memory with id=%d...", id)
					memory.Clear(id) // TODO: how not leak id?
				}
			}
		}(memory)
	}
	return storage
}

func (storage *Storage) Save(key string, value string) {
	storage.memory.Store(key, value, storage.dumpQueue)
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
		// logger.Printf("Found %s on disk", key)
	}
	return value, nil, fromMemory
}
