package sqlstream

import (
	"math/bits"
	"reflect"
	"sync"
)

const arbitraryCapacity = 8

func clearSlice[T any](sl []T) {
	sl = sl[:cap(sl)]
	var v T
	for i := range sl {
		sl[i] = v
	}
}

type sliceCache[T any] struct {
	slices [][]T
}

func (sc *sliceCache[T]) getSlice() (sl []T) {
	capacity := arbitraryCapacity
	if len(sc.slices) > 0 {
		sl = sc.slices[len(sc.slices)-1]
		sc.slices = sc.slices[:len(sc.slices)-1]
		capacity = 1 << bits.Len(uint(cap(sl)))
	}
	if len(sc.slices) > 0 {
		return
	}
	sc.slices = sc.slices[:cap(sc.slices)]
	sl = make([]T, (cap(sc.slices)+1)*capacity)
	for i := range sc.slices {
		start := i * capacity
		end := start + capacity
		sc.slices[i] = sl[start:start:end]
	}
	return sl[cap(sc.slices)*capacity:]
}

func (sc *sliceCache[T]) putSlice(sl []T) {
	sc.slices = append(sc.slices, sl)
}

var sliceCaches = sync.Map{} // map[reflect.Type]*sync.Pool[*sliceCache[T]]

func getSliceCachePool[T any]() *sync.Pool {
	var v T
	k := interface{}(reflect.TypeOf(v))
	pv, loaded := sliceCaches.Load(k)
	if loaded {
		return pv.(*sync.Pool)
	}
	p := &sync.Pool{
		New: func() any { return &sliceCache[T]{slices: make([][]T, 0, arbitraryCapacity)} },
	}
	if pv, loaded = sliceCaches.LoadOrStore(k, p); loaded {
		return pv.(*sync.Pool)
	}
	return p
}

func getSliceCache[T any]() *sliceCache[T] {
	return getSliceCachePool[T]().Get().(*sliceCache[T])
}

func putSliceCache[T any](sc *sliceCache[T]) {
	getSliceCachePool[T]().Put(sc)
}
