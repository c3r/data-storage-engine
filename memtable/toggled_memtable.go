package memtable

import (
	"strconv"
	"sync"
	"sync/atomic"
)

const MemtableMaxSize = 1000

type Memtable struct {
	Active bool
	Map    map[string]string
	Order  []string
	mutex  sync.Mutex
}

type ToggledMemtable struct {
	memtables map[string]*Memtable
	used      string
	mutex     sync.Mutex
	count     atomic.Uint64
}

func (tm *ToggledMemtable) Init() {
	tm.mutex.Lock()
	tm.memtables = make(map[string]*Memtable)
	tm.used = tm.nextId()
	tm.memtables[tm.used] = new(Memtable)
	tm.memtables[tm.used].init()
	tm.mutex.Unlock()
}

func (tm *ToggledMemtable) nextId() string {
	tm.count.Add(1)
	return strconv.Itoa(int(tm.count.Load()))
}

func (tm *ToggledMemtable) Rotate() string {
	tm.mutex.Lock()
	prev := tm.used
	tm.used = tm.nextId()
	tm.memtables[tm.used] = new(Memtable)
	tm.memtables[tm.used].init()
	tm.mutex.Unlock()
	return prev
}

func (tm *ToggledMemtable) IsMaxSize() bool {
	return tm.memtables[tm.used].isMaxSize()
}

func (tm *ToggledMemtable) Put(key string, value string) {
	tm.mutex.Lock()
	tm.memtables[tm.used].put(key, value)
	tm.mutex.Unlock()
}

func (tm *ToggledMemtable) Get(key string) (string, bool) {
	tm.mutex.Lock()
	for _, memtable := range tm.memtables {
		value, valueExists := memtable.get(key)
		if valueExists {
			return value, true
		}
	}
	tm.mutex.Unlock()
	return "", false
}

func (tm *ToggledMemtable) Clear(memtableKey string) {
	tm.memtables[memtableKey].clear()
	delete(tm.memtables, memtableKey)
}

func (tm *ToggledMemtable) Dump(memtableKey string) ([]string, map[string]string) {
	tm.mutex.Lock()
	order := tm.memtables[memtableKey].order()
	table := tm.memtables[memtableKey].table()
	tm.mutex.Unlock()
	return order, table
}
