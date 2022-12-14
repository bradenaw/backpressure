package backpressure

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/bradenaw/juniper/xsync"
)

// Semaphore is a way to bound concurrency and similar to golang.org/x/sync/semaphore, with two
// major differences:
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
	shortTimeout          time.Duration
	debtForgivePerSuccess float64
	bg                    *xsync.Group

	m           sync.Mutex
	capacity    int
	outstanding int
	queues      []codel[int]
	debt        []expDecay
}

// Additional options for the Semaphore type. These options do not frequently need to be tuned as
// the defaults work in a majority of cases, but they're included for completeness.
type SemaphoreOption struct{ f func(*semaphoreOptions) }

type semaphoreOptions struct {
	shortTimeout          time.Duration
	longTimeout           time.Duration
	debtDecayPctPerSec    float64
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

// The percentage by which debt decays per second, in [0, 1]. Debt decays exponentially over time,
// since load patterns change and a previously learned debt amount may no longer be relevant.
func SemaphoreDebtDecayPctPerSec(x float64) SemaphoreOption {
	return SemaphoreOption{func(opts *semaphoreOptions) {
		opts.debtDecayPctPerSec = x
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
	opts := semaphoreOptions{
		shortTimeout:          5 * time.Millisecond,
		longTimeout:           100 * time.Millisecond,
		debtDecayPctPerSec:    0.05,
		debtForgivePerSuccess: 0.1,
	}
	for _, option := range options {
		option.f(&opts)
	}
	now := time.Now()

	s := &Semaphore{
		shortTimeout:          opts.shortTimeout,
		debtForgivePerSuccess: opts.debtForgivePerSuccess,
		bg:                    xsync.NewGroup(context.Background()),

		capacity:    capacity,
		queues:      make([]codel[int], priorities),
		debt:        make([]expDecay, priorities),
		outstanding: 0,
	}

	for i := range s.queues {
		s.queues[i] = newCodel[int](opts.shortTimeout, opts.longTimeout)
		s.debt[i] = expDecay{
			decay: opts.debtDecayPctPerSec,
			last:  now,
		}
	}
	s.bg.Once(s.background)

	return s
}

// Acquire attempts to acquire some number of tokens from the semaphore on behalf of the given
// priority. If Acquire returns nil, these tokens should be returned to the semaphore when the
// caller is finished with them by using Release. Acquire returns non-nil if the given context
// expires before the tokens can be acquired, or if the request is rejected for timing out with the
// semaphore's own timeout.
func (s *Semaphore) Acquire(ctx context.Context, p Priority, tokens int) error {
	now := time.Now()
	s.m.Lock()
	allEmpty := true
	for i := 0; i < int(p); i++ {
		if !s.queues[i].empty() {
			allEmpty = false
			break
		}
	}
	if allEmpty && float64(s.outstanding)+s.debt[p].get(now) < float64(s.capacity)-float64(tokens) {
		s.outstanding += tokens
		if s.outstanding > s.capacity {
			panic("unbalanced Semaphore.Acquire and Semaphore.Release")
		}
		for i := int(p) + 1; i < len(s.debt); i++ {
			s.debt[i].add(now, math.Max(
				-(s.debtForgivePerSuccess*float64(tokens)),
				-s.debt[i].get(now),
			))
		}
		s.m.Unlock()
		return nil
	}

	w := newCodelWaiter(now, tokens)
	s.queues[p].push(now, w)
	for i := int(p) + 1; i < len(s.debt); i++ {
		s.debt[i].add(now, math.Min(float64(tokens), float64(s.capacity)-s.debt[i].get(now)))
	}
	s.m.Unlock()

	err := w.wait(ctx)
	if err != nil {
		return err
	}
	return nil
}

// Release returns the given number of tokens to the semaphore. It should only be called if these
// tokens are known to be acquired from the semaphore with a corresponding Acquire.
func (s *Semaphore) Release(tokens int) {
	now := time.Now()

	s.m.Lock()
	s.outstanding -= tokens
	if s.outstanding < 0 {
		panic("unbalanced Semaphore.Acquire and Semaphore.Release")
	}
	s.admitLocked(now)
	s.m.Unlock()
}

// SetCapacity sets the maximum number of outstanding tokens for the semaphore. If more tokens than
// this new value are already outstanding the semaphore simply waits for them to be released, it has
// no way of recalling them. If the new capacity is higher than the old, this will immediately admit
// the waiters it can.
func (s *Semaphore) SetCapacity(capacity int) {
	now := time.Now()
	s.m.Lock()
	s.capacity = capacity
	s.admitLocked(now)
	s.m.Unlock()
}

func (s *Semaphore) background(ctx context.Context) {
	ticker := time.NewTicker(s.shortTimeout / 2)
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return
		case <-ticker.C:
			now := time.Now()
			s.m.Lock()
			for i := range s.queues {
				s.queues[i].reap(now)
			}
			s.admitLocked(now)
			s.m.Unlock()
		}
	}
}

// Close frees background resources used by the semaphore.
func (s *Semaphore) Close() {
	s.bg.Wait()
}

func (s *Semaphore) admitLocked(now time.Time) {
	for p := range s.queues {
		queue := &s.queues[p]
		for {
			nextTokens, ok := queue.next()
			if !ok {
				break
			}
			if !(float64(s.outstanding)+s.debt[p].get(now) < float64(s.capacity)-float64(nextTokens)) {
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
}
