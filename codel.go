package backpressure

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/bradenaw/juniper/container/deque"
)

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
		lastEmpty:    time.Now(),
		mode:         codelModeFIFO,
	}
}

// true if there are no waiters in the codel.
func (c *codel[T]) empty() bool {
	return c.items.Len() == 0
}

// returns the next item that would be removed by pop, if there is one. calling reap, setMode, or
// push invalidates this value.
func (c *codel[T]) peek() (T, bool) {
	if c.items.Len() == 0 {
		var zero T
		return zero, false
	}
	switch c.mode {
	case codelModeFIFO:
		return c.items.Front().t, true
	case codelModeLIFO:
		return c.items.Back().t, true
	default:
		panic("unreachable")
	}
}

// pop pops the next waiter from the codel. Returns zero, false if the next waiter has already been
// abandoned or if the queue is empty.
//
// popping the next still-waiting waiter requires doing this inside of a loop checking if the codel
// is empty.
func (c *codel[T]) pop(now time.Time) (T, bool) {
	var zero T
	if c.items.Len() == 0 {
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
		return zero, false
	}
	return w.t, true
}

// push adds a waiter to the codel.
func (c *codel[T]) push(now time.Time, w *codelWaiter[T]) {
	c.setMode(now)
	c.items.PushBack(w)
}

// reap removes and wakes-unsuccessfully waiters that have timed out based on the current codelMode.
// codel assumes this will be called at least once every longTimeout.
func (c *codel[T]) reap(now time.Time) {
	c.setMode(now)
	timeout := c.longTimeout
	if c.mode == codelModeLIFO {
		timeout = c.shortTimeout
	}
	for c.items.Len() > 0 && now.Sub(c.items.Front().enqueued) > timeout {
		item := c.items.PopFront()
		item.drop()
	}
	c.setMode(now)
}

// setMode appropriately sets the codelMode. Called internally.
func (c *codel[T]) setMode(now time.Time) {
	if c.items.Len() == 0 {
		c.mode = codelModeFIFO
		c.lastEmpty = now
	} else if now.Sub(c.lastEmpty) > c.longTimeout {
		c.mode = codelModeLIFO
	}
}

type codelMode int

const (
	codelModeFIFO codelMode = iota
	codelModeLIFO
)

type codelWaiter[T any] struct {
	// time of enqueue, used for reap
	enqueued time.Time
	// signal for successful wake - admit needs to know if the waiter will wake successfully
	s uint32
	// sent true on successful wake, and false on unsuccessful
	c chan bool
	// extra state carried by the waiter
	t T
}

func newCodelWaiter[T any](now time.Time, t T) *codelWaiter[T] {
	return &codelWaiter[T]{
		enqueued: now,
		s:        0,
		c:        make(chan bool, 1),
		t:        t,
	}
}

// wait blocks until the waiter is woken up by the codel or ctx expires. returns ErrRejected if
// timed out by the codel.
func (w *codelWaiter[T]) wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		// They already decided to wake us, so we can't expire yet.
		if !atomic.CompareAndSwapUint32(&w.s, 0, 2) {
			return nil
		}
		return ctx.Err()
	case v := <-w.c:
		if v {
			return nil
		} else {
			return ErrRejected
		}
	}
}

// admit attempts to successfully-wake the waiter, returns true if the waiter will return nil from
// wait.
func (w *codelWaiter[T]) admit() bool {
	ok := atomic.CompareAndSwapUint32(&w.s, 0, 1)
	w.c <- true
	return ok
}

// drop unsuccessfully-wakes the waiter, it will return ErrRejected from wait.
func (w *codelWaiter[T]) drop() {
	w.c <- false
}
