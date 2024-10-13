package memtable

import (
	"sync"
	"sync/atomic"

	"github.com/c3r/data-storage-engine/syncmap"
)

// var logger = log.Default()

type mtable struct {
	syncMap *syncmap.SynchronizedMap[string, string]
	length  atomic.Int64
}

type MemtableManager struct {
	memtables         *syncmap.SynchronizedMap[int64, *mtable] // TODO: order matters
	currentMemtableId atomic.Int64
	currentMemtable   *mtable
	maxMemtableSize   int64
	mutex             sync.Mutex
	dumpQueue         chan int64
}

func New(maxMtableSize int64, dumpQueueSize int64) *MemtableManager {
	mgr := &MemtableManager{
		syncmap.New[int64, *mtable](),
		atomic.Int64{},
		nil,
		maxMtableSize,
		sync.Mutex{},
		make(chan int64, dumpQueueSize),
	}
	mtable1 := &mtable{syncmap.New[string, string](), atomic.Int64{}}
	mtable2 := &mtable{syncmap.New[string, string](), atomic.Int64{}}
	mgr.memtables.Store(int64(0), mtable1)
	mgr.memtables.Store(int64(1), mtable2)
	mgr.currentMemtable = mtable1
	return mgr
}

func (mgr *MemtableManager) Store(key string, value string) {
	// This lock is very bad for storing performance
	// TODO: Try to figure out how to continue saving when maxMtableSize is reached
	mgr.mutex.Lock()
	mgr.currentMemtable.syncMap.Store(key, value)
	if mgr.currentMemtable.length.Add(1) == mgr.maxMemtableSize {
		mgr.dumpQueue <- mgr.currentMemtableId.Load()
		if currMemtable, ok := mgr.memtables.Load(mgr.currentMemtableId.Load() + 1); ok {
			mgr.currentMemtable = currMemtable
			mgr.currentMemtableId.Add(1)
			memtable := &mtable{syncMap: syncmap.New[string, string]()}
			mgr.memtables.Store(mgr.currentMemtableId.Load()+1, memtable)
		}
	}
	mgr.mutex.Unlock()
}

func (mgr *MemtableManager) Load(key string) (string, bool) {
	if value, valueExists := mgr.currentMemtable.syncMap.Load(key); valueExists {
		return value, valueExists
	} else {
		// TODO: order matters!
		mgr.memtables.Range(func(_ int64, memtable *mtable) bool {
			value, valueExists = memtable.syncMap.Load(key)
			return !valueExists
		})
		return value, valueExists
	}
}

func (mgr *MemtableManager) Clear(id int64) {
	mgr.memtables.Delete(id)
}

func (mgr *MemtableManager) Dump() (int64, *syncmap.SynchronizedMap[string, string]) {
	memtableId := <-mgr.dumpQueue
	if memtable, valueExists := mgr.memtables.Load(memtableId); valueExists {
		return memtableId, memtable.syncMap
	}
	return -1, nil
}
