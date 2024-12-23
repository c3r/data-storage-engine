package storage

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	seg "github.com/c3r/data-storage-engine/segment"
	sm "github.com/c3r/data-storage-engine/syncmap"
)

type (
	Memtable = *sync.Map
)

func _newDataTable() Memtable {
	return &sync.Map{}
}

type Segments struct {
	*sm.Ordered[int64, *seg.Segment]
}

func _newSegments() Segments {
	return Segments{sm.New[int64, *seg.Segment]()}
}

type Memtables struct {
	*sm.Ordered[int64, Memtable]
}

func _newMemtable() Memtables {
	return Memtables{sm.New[int64, Memtable]()}
}

type Storage struct {
	currId    atomic.Int64
	memtables Memtables
	segments  Segments
	dir       string
	maxLen    int64
	currLen   atomic.Int64
	mutex     sync.Mutex
	queue     chan int64
}

func NewStorage(maxSegmentSize int64, maxSegments int64, segThreads int, dirPath string, compactPeriod int) *Storage {
	storage := &Storage{
		currId:    atomic.Int64{},
		memtables: _newMemtable(),
		segments:  _newSegments(),
		dir:       dirPath,
		maxLen:    maxSegmentSize,
		mutex:     sync.Mutex{},
		queue:     make(chan int64, maxSegments),
	}
	storage.memtables.Store(int64(0), _newDataTable())

	// Start segmentation threads
	for range segThreads {
		go func() {
			for {
				id := <-storage.queue
				table, ok := storage.memtables.Load(id)
				if !ok {
					msg := fmt.Sprintf("memtable with id %d does not exist", id)
					panic(msg)
				}

				segment, err := seg.Create(id, table, storage.dir)
				if err != nil {
					panic(err)
				}
				storage.segments.Store(id, segment)
				log.Printf("New segment created, segments: %d", storage.segments.Size())
				storage.memtables.Delete(id)
			}
		}()
	}

	// Start compaction thread
	go func() {
		var err error
		var merged *seg.Segment
		compact_segments := func(segment1 *seg.Segment, segment2 *seg.Segment) {
			defer func() {
				if err = segment1.Delete(); err != nil {
					log.Printf("Cannot delete segment %d: %s", segment1.Id, err.Error())
				}
				if err = segment2.Delete(); err != nil {
					log.Printf("Cannot delete segment %d: %s", segment1.Id, err.Error())
				}
			}()
			log.Printf("Compacting segments %d and %d...", segment1.Id, segment2.Id)
			merged, err = segment1.MergeWith(segment2, storage.dir)
			if err != nil {
				log.Printf("Error while compacting: %s", err.Error())
				return
			}
			storage.segments.Delete(segment1.Id)
			storage.segments.Swap(segment2.Id, merged)
			log.Printf("Segments %d and %d compacted. Segments: %d", segment1.Id, segment2.Id, storage.segments.Size())
		}
		var firstId int64
		var otherSegment *seg.Segment
		var valueExists bool
		for {
			time.Sleep(time.Duration(compactPeriod) * time.Second)
			size := storage.segments.Size()
			if size < 2 {
				continue
			}
			firstId = -1
			for segment := range storage.segments.ForValues {
				if segment.Id == firstId {
					continue
				}
				if firstId == -1 {
					firstId = segment.Id
					continue
				}
				if otherSegment, valueExists = storage.segments.Load(firstId); !valueExists {
					log.Printf("Error while compacting: segment %d does not exist", firstId)
					continue
				}
				if segment.Size() != otherSegment.Size() {
					log.Printf("%d and %d are different sizes (%d != %d)", segment.Id, otherSegment.Id, segment.Size(), otherSegment.Size())
					firstId = segment.Id
					continue
				}
				compact_segments(otherSegment, segment)
				firstId = -1
			}
		}
	}()

	// Load persisted segments into memory
	// if segments, err := segment.LoadPersistedSegments(storage.dir); err == nil {
	// 	for _, segment := range segments {
	// 		storage.segments.Store(segment.Id, segment)
	// 	}
	// }
	return storage
}

func (storage *Storage) Save(key string, value string) error {
	var table *sync.Map
	var valueExists bool
	storage.mutex.Lock()
	id := storage.currId.Load()
	if table, valueExists = storage.memtables.Load(id); !valueExists {
		return fmt.Errorf("no current memtable")
	}
	defer storage.mutex.Unlock()
	table.Store(key, value)
	storage.currLen.Add(1)
	if storage.currLen.Load() == storage.maxLen {
		storage.queue <- id
		storage.memtables.Store(storage.currId.Load()+1, _newDataTable())
		storage.currId.Add(1)
		storage.currLen.Swap(0)
	}
	return nil
}

func (storage *Storage) Load(key string) (string, error, bool) {
	for table := range storage.memtables.ForValues {
		value, valueExists := table.Load(key)
		if valueExists {
			return value.(string), nil, true
		}
	}
	for segment := range storage.segments.Reverse {
		value, valueExists, err := segment.Load(key)
		if err != nil {
			return "", err, false
		}
		if valueExists {
			return value, nil, false
		}
	}
	return "", fmt.Errorf("value not found for %s", key), false
}
