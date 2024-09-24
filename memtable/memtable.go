package memtable

import (
	"log"
	"sync"
	"sync/atomic"
)

var logger = log.Default()

type queue []int64

func (queue *queue) Push(element int64) {
	*queue = append(*queue, element)
}

func (queue *queue) Pop() (int64, bool) {
	head := *queue
	length := len(head)
	if length == 0 {
		return 0, false
	}
	var element int64
	element, *queue = head[0], head[1:length]
	return element, true
}

type mtable struct {
	syncMap *sync.Map
	length  atomic.Int64
}

type MemtableManager struct {
	memtables       *sync.Map
	currentId       atomic.Int64
	currentMemtable *mtable
	maxSize         int64
	removals        *queue
	mutex           sync.Mutex
}

func New(maxSize int64) *MemtableManager {
	manager := &MemtableManager{&sync.Map{}, atomic.Int64{}, nil, maxSize, &queue{}, sync.Mutex{}}
	memtable := &mtable{&sync.Map{}, atomic.Int64{}}
	manager.memtables.Store(0, memtable)
	manager.currentMemtable = memtable
	return manager
}

func (manager *MemtableManager) Store(key string, value string, dumpQueue chan int64) {
	manager.mutex.Lock()
	manager.currentMemtable.syncMap.Store(key, value)
	id := manager.currentId.Load()
	if manager.currentMemtable.length.Add(1) >= manager.maxSize {
		memtable := &mtable{syncMap: &sync.Map{}}
		manager.memtables.Store(manager.currentId.Add(1), memtable)
		manager.currentMemtable = memtable
		dumpQueue <- id
		logger.Printf("Current dump queue length: %d", len(dumpQueue))
	}
	manager.mutex.Unlock()
}

func (manager *MemtableManager) Load(key string) (string, bool) {
	var value any
	var valueExists bool
	if value, valueExists = manager.currentMemtable.syncMap.Load(key); !valueExists {
		manager.memtables.Range(func(_, memtable any) bool {
			value, valueExists = memtable.(*mtable).syncMap.Load(key)
			return !valueExists
		})
		if value == nil {
			value = ""
		}
	}
	return value.(string), valueExists
}

func (manager *MemtableManager) Clear(id int64) {
	manager.memtables.Delete(id)
}

func (manager *MemtableManager) Dump(id int64) (*sync.Map, bool) {
	if value, valueExists := manager.memtables.Load(id); valueExists {
		mtable := value.(*mtable)
		return mtable.syncMap, valueExists
	}
	return nil, false
}
