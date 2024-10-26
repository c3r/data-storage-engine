package syncmap

import (
	"cmp"
	"slices"
	"sync"
)

type Ordered[K cmp.Ordered, V any] struct {
	data  *sync.Map
	order []K
	mtx   sync.Mutex
}

func New[K cmp.Ordered, V any]() *Ordered[K, V] {
	return &Ordered[K, V]{
		data:  &sync.Map{},
		order: []K{},
		mtx:   sync.Mutex{},
	}
}

func (s *Ordered[K, V]) Size() int64 {
	return int64(len(s.order))
}

func (s *Ordered[K, V]) LoadOrdered(idx int64) (V, bool) {
	k := s.order[idx]
	v, valueExists := s.data.Load(k)
	return v.(V), valueExists
}

func (s *Ordered[K, V]) Load(k K) (V, bool) {
	var r V
	v, valueExists := s.data.Load(k)
	if !valueExists {
		return r, false
	}
	r = v.(V)
	return r, true
}

func (s *Ordered[K, V]) Store(k K, v V) {
	s.data.Store(k, v)
	s.appendOrder(k)
}

func (s *Ordered[K, V]) Swap(k K, v V) {
	s.data.Swap(k, v)
	s.deleteOrder(k)
	s.appendOrder(k)
}

func (s *Ordered[K, V]) Delete(k K) {
	s.data.Delete(k)
	s.deleteOrder(k)
}

func (s *Ordered[K, V]) appendOrder(k K) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.order = append(s.order, k)
	slices.Sort(s.order)
}

func (s *Ordered[K, V]) deleteOrder(k K) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	for ord, key := range s.order {
		if key == k {
			s.order = append(s.order[:ord], s.order[ord+1:]...)
			break
		}
	}
}

func (s *Ordered[K, V]) Entries(f func(k K, v V) bool) {
	for _, k := range s.order {
		v, valueExists := s.data.Load(k)
		if !valueExists {
			continue
		}
		if !f(k, v.(V)) {
			break
		}
	}
}

func (s *Ordered[K, V]) Keys(f func(k K) bool) {
	for _, k := range s.order {
		if !f(k) {
			break
		}
	}
}

func (s *Ordered[K, V]) Values(f func(v V) bool) {
	for _, k := range s.order {
		v, valueExists := s.data.Load(k)
		if !valueExists {
			continue
		}
		if !f(v.(V)) {
			break
		}
	}
}

func (s *Ordered[K, V]) ValuesReverse(f func(value V) bool) {
	for i := len(s.order) - 1; i >= 0; i-- {
		k := s.order[i]
		v, valueExists := s.data.Load(k)
		if !valueExists {
			continue
		}
		if !f(v.(V)) {
			break
		}
	}
}
