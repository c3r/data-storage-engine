package storage

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/c3r/data-storage-engine/files"
	seg "github.com/c3r/data-storage-engine/segment"
	sm "github.com/c3r/data-storage-engine/syncmap"
	"github.com/google/uuid"
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
	for range segThreads {
		go func() {
			for {
				id := <-storage.queue
				table, ok := storage.memtables.Load(id)
				if !ok {
					msg := fmt.Sprintf("memtable with id %d does not exist", id)
					panic(msg)
				}
				filePath := fmt.Sprintf(storage.dir + "/" + uuid.New().String()) // TODO: handle slash ending
				segment, err := seg.CreateSegment(id, table, filePath)
				if err != nil {
					panic(err)
				}
				storage.segments.Store(id, segment)
				log.Printf("New segment created, segments: %d", storage.segments.Size())
				storage.memtables.Delete(id)
			}
		}()
	}
	go func() {
		for {
			time.Sleep(time.Duration(compactPeriod) * time.Second)
			size := storage.segments.Size()
			if size >= 2 {
				if older, valueExists := storage.segments.LoadOrdered(size - 2); valueExists {
					if newer, valueExists := storage.segments.LoadOrdered(size - 1); valueExists {
						if newer.Size() != older.Size() {
							continue
						}
						filePath := fmt.Sprintf(storage.dir + "/" + uuid.New().String())
						merged, err := older.Merge(newer, filePath)
						if err != nil {
							log.Println("Error while compacting")
							continue
						}
						storage.segments.Delete(older.Id)
						storage.segments.Swap(newer.Id, merged)
						go func() {
							older.Wait()
							files.Delete(older.FilePath)
						}()
						go func() {
							newer.Wait()
							files.Delete(newer.FilePath)
						}()
					}
				}
			} else {
				// log.Println("Not enough segments")
			}
		}
	}()

	// Load segments from disk
	// entries, err := os.ReadDir(segmentsDirPath)
	// if err != nil {
	// 	return err
	// }
	// for _, directoryEntry := range entries {
	// 	if !directoryEntry.Type().IsRegular() {
	// 		continue
	// 	}
	// 	filePath := fmt.Sprintf("%s/%s", dirPath, directoryEntry.Name())
	// 	segment, err := loadSegmentFromFile(filePath)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	storage.segments.Store(segment.id, segment)
	// }
	// End load segments from disk
	return storage
}

func (storage *Storage) removeSegment(id int64) {
	if segment, ok := storage.segments.Load(id); ok {
		if err := files.Delete(segment.FilePath); err != nil {
			storage.segments.Delete(id)
		}
	}
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
	if val, valueExists = storage.tablesSearch(key); !valueExists {
		isMemory = false
		if val, valueExists = storage.segmentsSearch(key); !valueExists {
			return val, fmt.Errorf("value not found for %s", key), isMemory
		}
	}
	return val, nil, isMemory
}

func (storage *Storage) segmentsSearch(key string) (string, bool) {
	var val string
	var valueExists bool
	var err error
	storage.segments.ValuesReverse(func(segment *seg.Segment) bool {
		val, valueExists, err = segment.Load(key)
		return err == nil && !valueExists
	})
	return val, valueExists
}

func (storage *Storage) tablesSearch(key string) (string, bool) {
	var val string
	var valueExists bool
	storage.memtables.Values(func(table Memtable) bool {
		val, valueExists = table.Load(key)
		return !valueExists
	})
	return val, valueExists
}
