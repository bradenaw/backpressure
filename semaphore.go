package backpressure

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Semaphore is a way to bound concurrency and similar to golang.org/x/sync/semaphore. Conceptually,
// it is a bucket of some number of tokens. Callers can take tokens out of this bucket using
// Acquire, do whatever operation needs concurrency bounding, and then return the tokens with
// Release. If the bucket does not have enough tokens in it to Acquire, it will block for some time
// in case another user of tokens Releases.
//
// It has two major differences from golang.org/x/sync/semaphore:
//
// 1. It is prioritized, preferring to accept higher priority requests first.
//
// 2. Each queue of waiters is a CoDel, which is not fair but can behave better in a real-time
// system under load.
//
// In order to minimize wait times for high-priority requests, it self balances using "debt." Debt
// is tracked per priority and is the number of tickets that must be left in the semaphore before a
// given request may be admitted.
//
// Debt is self-adjusting: whenever a high-priority `Acquire()` cannot immediately be accepted, the
// debt for all lower priorities is increased. Intuitively, this request would not have had to wait
// if this debt already existed, so the semaphore self-corrects by adding it. Whenever a
// high-priority `Acquire()` can be admitted without waiting, then any existing debt may not have
// been necessary and so some of it is forgiven. Additionally, debt decays over time, since anything
// the semaphore has learned about a load pattern may become out-of-date as load changes.
type Semaphore struct {
	longTimeout           time.Duration
	debtForgivePerSuccess float64
	debtDecayInterval     time.Duration

	m      sync.Mutex
	bgDone chan struct{}
	closed bool
	// Set to longTimeout if there are any waiters, or forever otherwise.
	reapTicker *time.Ticker
	// The capacity of the semaphore in number of tokens.
	capacity int
	// Number of tokens currently outstanding: those that we've granted to Acquire()s but have not
	// yet been Release()d.
	outstanding int
	// Are there any waiters in queues?
	hasWaiters bool
	// Queues by priority.
	queues []codel[int]
	// Debts by priority.
	debt []linearDecay
}

// Additional options for the Semaphore type. These options do not frequently need to be tuned as
// the defaults work in a majority of cases, but they're included for completeness.
type SemaphoreOption struct{ f func(*semaphoreOptions) }

type semaphoreOptions struct {
	shortTimeout          time.Duration
	longTimeout           time.Duration
	debtDecayInterval     time.Duration
	debtForgivePerSuccess float64
}

// The short timeout for the internal CoDels. See the README for more on CoDel.
func SemaphoreShortTimeout(d time.Duration) SemaphoreOption {
	return SemaphoreOption{func(opts *semaphoreOptions) {
		opts.shortTimeout = d
	}}
}

// The long timeout for the internal CoDels. See the README for more on CoDel.
func SemaphoreLongTimeout(d time.Duration) SemaphoreOption {
	return SemaphoreOption{func(opts *semaphoreOptions) {
		opts.longTimeout = d
	}}
}

// The time it takes for 100% debt to be completely forgiven. Debt decays linearly over time since
// load patterns change and a previously learned debt amount may no longer be relevant.
func SemaphoreDebtDecayInterval(x time.Duration) SemaphoreOption {
	return SemaphoreOption{func(opts *semaphoreOptions) {
		opts.debtDecayInterval = x
	}}
}

// The proportion of debt that is forgiven for lower priorities whenever a higher-priority request
// succeeds, in [0, 1].
func SemaphoreDebtForgivePerSuccess(x float64) SemaphoreOption {
	return SemaphoreOption{func(opts *semaphoreOptions) {
		opts.debtForgivePerSuccess = x
	}}
}

// NewSemaphore returns a semaphore with the given number of priorities, and will allow at most
// capacity concurrency.
//
// The other options do not frequently need to be modified.
func NewSemaphore(
	priorities int,
	capacity int,
	options ...SemaphoreOption,
) *Semaphore {
	if capacity < 0 {
		panic("negative capacity")
	}

	opts := semaphoreOptions{
		shortTimeout:          5 * time.Millisecond,
		longTimeout:           100 * time.Millisecond,
		debtDecayInterval:     10 * time.Second,
		debtForgivePerSuccess: 0.1,
	}
	for _, option := range options {
		option.f(&opts)
	}
	now := time.Now()

	s := &Semaphore{
		longTimeout:           opts.longTimeout,
		debtForgivePerSuccess: opts.debtForgivePerSuccess,
		debtDecayInterval:     opts.debtDecayInterval,

		reapTicker:  time.NewTicker(forever),
		capacity:    capacity,
		queues:      make([]codel[int], priorities),
		debt:        make([]linearDecay, priorities),
		hasWaiters:  false,
		outstanding: 0,
	}

	for i := range s.queues {
		s.queues[i] = newCodel[int](opts.shortTimeout, opts.longTimeout)
		s.debt[i] = linearDecay{
			decayPerSec: float64(capacity) / opts.debtDecayInterval.Seconds(),
			last:        now,
			max:         float64(capacity),
		}
	}

	return s
}

// Acquire attempts to acquire some number of tokens from the semaphore on behalf of the given
// priority. If Acquire returns nil, these tokens should be returned to the semaphore when the
// caller is finished with them by using Release. Acquire returns non-nil if the given context
// expires before the tokens can be acquired, or if the request is rejected for timing out with the
// semaphore's own timeout.
func (s *Semaphore) Acquire(ctx context.Context, p Priority, tokens int) error {
	s.m.Lock()

	if s.closed {
		s.m.Unlock()
		return errAlreadyClosed
	}
	if s.capacity == 0 {
		s.m.Unlock()
		return errZeroCapacity
	}

	if tokens > s.capacity {
		s.m.Unlock()
		return fmt.Errorf(
			"tried to Acquire %d tokens, semaphore only has capacity for %d",
			tokens,
			s.capacity,
		)
	}

	now := time.Now()
	hasHigherPriority := false
	for i := 0; i < int(p); i++ {
		if !s.queues[i].empty() {
			hasHigherPriority = true
			break
		}
	}

	// There's enough capacity to admit right away.
	if !hasHigherPriority && s.canAdmit(now, p, tokens) {
		s.outstanding += tokens
		if s.outstanding > s.capacity {
			panic("unbalanced Semaphore.Acquire and Semaphore.Release")
		}
		for i := int(p) + 1; i < len(s.debt); i++ {
			s.debt[i].add(now, -(s.debtForgivePerSuccess * float64(tokens)))
		}
		s.m.Unlock()
		return nil
	}

	// Penalize the lower-priorities for making this request wait.
	for i := int(p) + 1; i < len(s.debt); i++ {
		s.debt[i].add(now, float64(tokens))
	}

	w := newCodelWaiter(now, tokens)
	s.queues[p].push(now, w)
	if !s.hasWaiters {
		s.reapTicker.Reset(s.longTimeout)
		s.hasWaiters = true
	}
	if s.bgDone == nil {
		s.bgDone = make(chan struct{})
		go s.background()
	}
	s.admit(now)
	s.m.Unlock()

	return w.wait(ctx)
}

// Release returns the given number of tokens to the semaphore. It should only be called if these
// tokens are known to be acquired from the semaphore with a corresponding Acquire.
func (s *Semaphore) Release(tokens int) {
	s.m.Lock()
	s.outstanding -= tokens
	if s.outstanding < 0 {
		panic("unbalanced Semaphore.Acquire and Semaphore.Release")
	}
	s.admit(time.Now())
	s.m.Unlock()
}

// SetCapacity sets the maximum number of outstanding tokens for the semaphore. If more tokens than
// this new value are already outstanding the semaphore simply waits for them to be released, it has
// no way of recalling them. If the new capacity is higher than the old, this will immediately admit
// the waiters it can.
func (s *Semaphore) SetCapacity(capacity int) {
	if capacity < 0 {
		panic("negative capacity")
	}
	s.m.Lock()
	now := time.Now()
	s.capacity = capacity
	for i := range s.debt {
		s.debt[i].setDecayPerSec(now, float64(capacity)/s.debtDecayInterval.Seconds())
		s.debt[i].setMax(now, float64(s.capacity))
	}
	s.admit(now)
	s.m.Unlock()
}

func (s *Semaphore) background() {
	for {
		select {
		case <-s.bgDone:
			return
		case <-s.reapTicker.C:
			s.m.Lock()
			if s.reapTicker == nil {
				s.m.Unlock()
				return
			}
			now := time.Now()
			s.admit(now)
			s.m.Unlock()
		}
	}
}

// Close frees background resources used by the semaphore.
func (s *Semaphore) Close() {
	s.m.Lock()
	defer s.m.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	s.reapTicker.Stop()
	if s.bgDone != nil {
		close(s.bgDone)
	}
}

func (s *Semaphore) canAdmit(now time.Time, p Priority, tokens int) bool {
	available := float64(s.capacity) - float64(s.outstanding)
	debt := s.debt[int(p)].get(now)
	// Rule is that there need to be at least `debt` tokens left over after we admit this.
	return available > float64(tokens)+debt
}

func (s *Semaphore) admit(now time.Time) {
	for p := range s.queues {
		s.queues[p].reap(now)
	}
	for p := range s.queues {
		queue := &s.queues[p]
		for {
			nextTokens, ok := queue.peek()
			if !ok {
				break
			}
			if !s.canAdmit(now, Priority(p), nextTokens) {
				break
			}
			_, ok = queue.pop(now)
			if ok {
				s.outstanding += nextTokens
			}
		}
		if !queue.empty() {
			return
		}
	}

	// All queues are empty.
	if s.hasWaiters {
		s.reapTicker.Reset(forever)
		s.hasWaiters = false
	}
}
