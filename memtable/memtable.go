package memtable

import "slices"

func (memory *Memtable) init() {
	memory.Active = true
	memory.clear()
}

func (memory *Memtable) isMaxSize() bool {
	return len(memory.Map) == MemtableMaxSize
}

func (memory *Memtable) put(key string, value string) {
	memory.mutex.Lock()
	memory.Map[key] = value
	memory.Order = append(memory.Order, key)
	slices.Sort(memory.Order)
	memory.mutex.Unlock()
}

func (memory *Memtable) get(key string) (string, bool) {
	value, valueExists := memory.Map[key]
	return value, valueExists
}

func (memory *Memtable) clear() {
	memory.Map = make(map[string]string)
	memory.Order = []string{}
}

func (memory *Memtable) order() []string {
	return memory.Order
}

func (memory *Memtable) table() map[string]string {
	return memory.Map
}
