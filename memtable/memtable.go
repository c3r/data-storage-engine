package memtable

import (
	"log"
	"sync"
	"sync/atomic"
)

var logger = log.Default()

type mtable struct {
	syncMap *sync.Map
	length  atomic.Int64
}

type MemtableManager struct {
	mtables       *sync.Map
	currId        atomic.Int64
	currMtable    *mtable
	maxMtableSize int64
	mutex         sync.Mutex
	dumpQueue     chan int64
}

func New(maxSize int64, maxMtables int64) *MemtableManager {
	mgr := &MemtableManager{
		&sync.Map{},
		atomic.Int64{},
		nil,
		maxSize,
		sync.Mutex{},
		make(chan int64, maxMtables),
	}
	mtable := &mtable{&sync.Map{}, atomic.Int64{}}
	mgr.mtables.Store(int64(0), mtable)
	mgr.currMtable = mtable
	return mgr
}

func (mgr *MemtableManager) Store(key string, value string) {
	mgr.mutex.Lock()
	mgr.currMtable.syncMap.Store(key, value)
	if mgr.currMtable.length.Add(1) >= mgr.maxMtableSize {
		id := mgr.currId.Load()
		memtable := &mtable{syncMap: &sync.Map{}}
		mgr.mtables.Store(mgr.currId.Add(1), memtable)
		mgr.currMtable = memtable
		mgr.dumpQueue <- id
	}
	mgr.mutex.Unlock()
}

func (mgr *MemtableManager) Load(key string) (string, bool) {
	var value any
	var valueExists bool
	if value, valueExists = mgr.currMtable.syncMap.Load(key); !valueExists {
		mgr.mtables.Range(func(_, memtable any) bool {
			value, valueExists = memtable.(*mtable).syncMap.Load(key)
			return !valueExists
		})
		if value == nil {
			value = ""
		}
	}
	return value.(string), valueExists
}

func (mgr *MemtableManager) Clear(id int64) {
	mgr.mtables.Delete(id)
}

func (mgr *MemtableManager) Dump() (int64, *sync.Map) {
	id := <-mgr.dumpQueue
	if memtable, valueExists := mgr.mtables.Load(id); valueExists {
		return id, memtable.(*mtable).syncMap
	}
	return -1, nil
}
