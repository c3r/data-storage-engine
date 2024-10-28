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
	Memtable = *sm.Ordered[string, string]
)

func _newDataTable() Memtable {
	return sm.New[string, string]()
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
				segment, err := seg.CreateSegment(id, table, storage.dir)
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
		for {
			time.Sleep(time.Duration(compactPeriod) * time.Second)
			size := storage.segments.Size()
			if size < 2 {
				continue
			}
			olderSegmentId := size - 2
			newerSegmentId := size - 1
			olderSegment, valueExists := storage.segments.LoadOrdered(olderSegmentId)
			if !valueExists {
				log.Printf("Error while compacting: segment %d does not exist", olderSegmentId)
				continue
			}
			newerSegment, valueExists := storage.segments.LoadOrdered(newerSegmentId)
			if !valueExists {
				log.Printf("Error while compacting: segment %d does not exist", newerSegmentId)
				continue
			}
			if newerSegment.Size() != olderSegment.Size() {
				continue
			}
			merged, err := olderSegment.Merge(newerSegment, storage.dir)
			if err != nil {
				log.Printf("Error while compacting: %s", err.Error())
				continue
			}
			storage.segments.Delete(olderSegment.Id)
			storage.segments.Swap(newerSegment.Id, merged)
			if err = olderSegment.Delete(); err != nil {
				log.Printf("Cannot delete segment %d: %s", olderSegment.Id, err.Error())
			}
			if err = newerSegment.Delete(); err != nil {
				log.Printf("Cannot delete segment %d: %s", olderSegment.Id, err.Error())
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

func (storage *Storage) Save(key string, value string) {
	// This lock is very bad for storing performance
	// TODO: Try to figure out how to continue saving when maxLen is reached
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	id := storage.currId.Load()
	if table, valueExists := storage.memtables.Load(id); valueExists {
		table.Store(key, value)
		if table.Size() == storage.maxLen {
			storage.queue <- id
			storage.memtables.Store(storage.currId.Load()+1, _newDataTable())
			storage.currId.Add(1)
		}
	} else {
		panic("no current memtable")
	}
}

func (storage *Storage) Load(key string) (string, error, bool) {
	var val string
	var valueExists bool
	var isMemory bool = true
	var err error
	if val, valueExists = storage.tablesSearch(key); !valueExists {
		isMemory = false
		if val, valueExists, err = storage.segmentsSearch(key); !valueExists {
			if err != nil {
				return val, err, isMemory
			}
			return val, fmt.Errorf("value not found for %s", key), isMemory
		}
	}
	return val, nil, isMemory
}

func (storage *Storage) segmentsSearch(key string) (string, bool, error) {
	var val string
	var valueExists bool
	var err error
	storage.segments.ForValuesReverse(func(segment *seg.Segment) bool {
		val, valueExists, err = segment.Load(key)
		return err == nil && !valueExists
	})
	return val, valueExists, err
}

func (storage *Storage) tablesSearch(key string) (string, bool) {
	var val string
	var valueExists bool
	storage.memtables.ForValues(func(table Memtable) bool {
		val, valueExists = table.Load(key)
		return !valueExists
	})
	return val, valueExists
}
