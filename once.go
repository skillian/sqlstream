package sqlstream

import (
	"sync"
	"sync/atomic"
)

type once struct {
	m    sync.Mutex
	done uint32
}

func (n *once) do(arg uintptr, f func(uintptr)) {
	if atomic.LoadUint32(&n.done) != 0 {
		return
	}
	n.m.Lock()
	if atomic.LoadUint32(&n.done) != 0 {
		n.m.Unlock()
		return
	}
	f(arg)
	n.m.Unlock()
	return
}
