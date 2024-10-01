package syncmap

import (
	"sync"
)

type SynchronizedMap[K any, V any] struct {
	syncMap sync.Map
}

func (syncmap *SynchronizedMap[K, V]) Load(key K) (V, bool) {
	value, _ := syncmap.syncMap.Load(key)
	return value.(V), true
}

func (syncmap *SynchronizedMap[K, V]) Store(key K, value V) {
	syncmap.syncMap.Store(key, value)
}

func (syncmap *SynchronizedMap[K, V]) Delete(key K) {
	syncmap.syncMap.Delete(key)
}

func (syncmap SynchronizedMap[K, V]) Range(f func(key K, value V) bool) {
	syncmap.syncMap.Range(func(key, value any) bool {
		return f(key.(K), value.(V))
	})
}
