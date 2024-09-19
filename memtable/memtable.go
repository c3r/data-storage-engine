package memtable

import (
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
)

type synchronizedKeyValueMap struct {
	keyMap map[string]string
	order  []string
	mutex  sync.Mutex
}

type Memtable struct {
	memtables map[string]*synchronizedKeyValueMap
	Used      string
	mutex     sync.Mutex
	maxSize   int
	Count     atomic.Uint64
}

func New(maxSize int) *Memtable {
	tm := &Memtable{
		memtables: make(map[string]*synchronizedKeyValueMap),
		maxSize:   maxSize,
		Count:     atomic.Uint64{},
		Used:      "0",
	}
	tm.initNext()
	return tm
}

func (tm *Memtable) Put(key string, value string) (string, bool) {
	m := tm.memtables[tm.Used]
	m.mutex.Lock()
	m.keyMap[key] = value
	m.order = append(m.order, key)
	slices.Sort(m.order)
	tm.mutex.Lock()
	if len(m.keyMap) == tm.maxSize {
		prev := tm.initNext()
		tm.mutex.Unlock()
		m.mutex.Unlock()
		return prev, true
	}
	m.mutex.Unlock()
	tm.mutex.Unlock()
	return "", false
}

func (tm *Memtable) Get(key string) (string, bool) {
	tm.mutex.Lock()
	for _, memtable := range tm.memtables {
		value, valueExists := memtable.keyMap[key]
		if valueExists {
			tm.mutex.Unlock()
			return value, true
		}
	}
	tm.mutex.Unlock()
	return "", false
}

func (tm *Memtable) Clear(memtableKey string) {
	tm.mutex.Lock()
	delete(tm.memtables, memtableKey)
	tm.mutex.Unlock()
}

func (tm *Memtable) Dump(id string) ([]string, map[string]string) {
	order := tm.memtables[id].order
	table := tm.memtables[id].keyMap
	return order, table
}

func (tm *Memtable) initNext() string {
	tm.Count.Add(1)
	prev := tm.Used
	tm.Used = strconv.Itoa(int(tm.Count.Load()))
	tm.memtables[tm.Used] = &synchronizedKeyValueMap{
		keyMap: make(map[string]string),
		order:  []string{},
	}
	return prev
}
