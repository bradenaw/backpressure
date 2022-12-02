package backpressure

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/bradenaw/juniper/container/deque"
)

type codelMode int

const (
	codelModeFIFO codelMode = iota
	codelModeLIFO
)

type codelWaiter[T any] struct {
	enqueued time.Time
	s        uint32
	c        chan bool
	t        T
}

func newCodelWaiter[T any](now time.Time, t T) *codelWaiter[T] {
	return &codelWaiter[T]{
		enqueued: now,
		s:        0,
		c:        make(chan bool, 1),
		t:        t,
	}
}

func (w *codelWaiter[T]) wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		if !atomic.CompareAndSwapUint32(&w.s, 0, 2) {
			return nil
		}
		return ctx.Err()
	case v := <-w.c:
		if v {
			return nil
		} else {
			return ErrTimedOut
		}
	}
}

func (w *codelWaiter[T]) admit() bool {
	ok := atomic.CompareAndSwapUint32(&w.s, 0, 1)
	w.c <- true
	return ok
}

func (w *codelWaiter[T]) drop() {
	w.c <- false
}

type codel[T any] struct {
	shortTimeout time.Duration
	longTimeout  time.Duration
	lastEmpty    time.Time
	mode         codelMode
	items        deque.Deque[*codelWaiter[T]]
}

func newCodel[T any](shortTimeout time.Duration, longTimeout time.Duration) codel[T] {
	return codel[T]{
		shortTimeout: shortTimeout,
		longTimeout:  longTimeout,
	}
}

func (c *codel[T]) setMode(now time.Time) {
	if c.items.Len() == 0 {
		c.mode = codelModeFIFO
	} else if now.Sub(c.lastEmpty) > c.longTimeout {
		c.mode = codelModeLIFO
	}
}

func (c *codel[T]) empty() bool {
	return c.items.Len() == 0
}

func (c *codel[T]) next() (T, bool) {
	if c.items.Len() == 0 {
		var zero T
		return zero, false
	}
	switch c.mode {
	case codelModeFIFO:
		return c.items.Item(0).t, true
	case codelModeLIFO:
		return c.items.Item(c.items.Len() - 1).t, true
	default:
		panic("unreachable")
	}
}

func (c *codel[T]) pop(now time.Time) (T, bool) {
	if c.items.Len() == 0 {
		var zero T
		return zero, false
	}
	var w *codelWaiter[T]
	switch c.mode {
	case codelModeFIFO:
		w = c.items.PopFront()
	case codelModeLIFO:
		w = c.items.PopBack()
	default:
		panic("unreachable")
	}
	c.setMode(now)
	ok := w.admit()
	if !ok {
		var zero T
		return zero, false
	}
	return w.t, true
}

func (c *codel[T]) push(now time.Time, w *codelWaiter[T]) {
	c.items.PushBack(w)
}

func (c *codel[T]) reap(now time.Time) {
	c.setMode(now)
	timeout := c.longTimeout
	if c.mode == codelModeLIFO {
		timeout = c.shortTimeout
	}
	for c.items.Len() > 0 {
		if now.Sub(c.items.Item(0).enqueued) <= timeout {
			break
		}
		item := c.items.PopFront()
		item.drop()
	}
	c.setMode(now)
}
