package memtable

import (
	"sync"
	"sync/atomic"

	"github.com/c3r/data-storage-engine/syncmap"
)

type segment struct {
	syncMap *syncmap.SynchronizedMap[string, string]
	length  atomic.Int64
}

func (segment *segment) store(key string, value string) int64 {
	segment.syncMap.Store(key, value)
	segment.length.Add(1)
	return segment.length.Load()
}

func createSegment() *segment {
	return &segment{syncMap: syncmap.New[string, string]()}
}

type Memtable struct {
	segmentMap       *syncmap.SynchronizedMap[int64, *segment] // TODO: order matters
	currentId        atomic.Int64
	currentSegment   *segment
	maxSegmentLength int64
	mutex            sync.Mutex
	dumpQueue        chan int64
}

func New(maxSegmentLength int64, dumpQueueSize int64) *Memtable {
	table := &Memtable{
		segmentMap:       syncmap.New[int64, *segment](),
		currentId:        atomic.Int64{},
		currentSegment:   nil,
		maxSegmentLength: maxSegmentLength,
		mutex:            sync.Mutex{},
		dumpQueue:        make(chan int64, dumpQueueSize),
	}
	table.currentSegment = createSegment()
	table.segmentMap.Store(int64(0), table.currentSegment)
	table.segmentMap.Store(int64(1), createSegment())
	return table
}

func (table *Memtable) Store(key string, value string) {
	// This lock is very bad for storing performance
	// TODO: Try to figure out how to continue saving when maxMtableSize is reached
	var dumpId int64
	table.mutex.Lock()
	if table.currentSegment.store(key, value) == table.maxSegmentLength {
		dumpId = table.currentId.Load()
		table.currentSegment, _ = table.segmentMap.Load(table.currentId.Add(1))
		table.mutex.Unlock()
		table.dumpQueue <- dumpId
		table.segmentMap.Store(table.currentId.Load()+1, createSegment())
	} else {
		table.mutex.Unlock()
	}
}

func (table *Memtable) Load(key string) (string, bool) {
	if value, valueExists := table.currentSegment.syncMap.Load(key); valueExists {
		return value, valueExists
	} else {
		// TODO: order matters!
		table.segmentMap.Range(func(_ int64, memtable *segment) bool {
			value, valueExists = memtable.syncMap.Load(key)
			return !valueExists
		})
		return value, valueExists
	}
}

func (table *Memtable) Clear(id int64) {
	table.segmentMap.Delete(id)
}

func (table *Memtable) Dump() (int64, *syncmap.SynchronizedMap[string, string]) {
	memtableId := <-table.dumpQueue
	if memtable, valueExists := table.segmentMap.Load(memtableId); valueExists {
		return memtableId, memtable.syncMap
	}
	return -1, nil
}
