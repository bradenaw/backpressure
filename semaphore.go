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
	queues      []codel[struct{}]
	debt        []expDecay
}

type SemaphoreTicket struct {
	parent *Semaphore
}

type SemaphoreOption struct{ f func(*semaphoreOptions) }

type semaphoreOptions struct {
	shortTimeout          time.Duration
	longTimeout           time.Duration
	debtDecayPctPerSec    float64
	debtForgivePerSuccess float64
}

func SemaphoreShortTimeout(d time.Duration) SemaphoreOption {
	return SemaphoreOption{func(opts *semaphoreOptions) {
		opts.shortTimeout = d
	}}
}

func SemaphoreLongTimeout(d time.Duration) SemaphoreOption {
	return SemaphoreOption{func(opts *semaphoreOptions) {
		opts.longTimeout = d
	}}
}

func SemaphoreDebtDecayPctPerSec(x float64) SemaphoreOption {
	return SemaphoreOption{func(opts *semaphoreOptions) {
		opts.debtDecayPctPerSec = x
	}}
}

func SemaphoreDebtForgivePerSuccess(x float64) SemaphoreOption {
	return SemaphoreOption{func(opts *semaphoreOptions) {
		opts.debtForgivePerSuccess = x
	}}
}

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
		queues:      make([]codel[struct{}], priorities),
		debt:        make([]expDecay, priorities),
		outstanding: 0,
	}

	for i := range s.queues {
		s.queues[i] = newCodel[struct{}](opts.shortTimeout, opts.longTimeout)
		s.debt[i] = expDecay{
			decay: opts.debtDecayPctPerSec,
			last:  now,
		}
	}
	s.bg.Once(s.background)

	return s
}

func (s *Semaphore) Acquire(ctx context.Context, p Priority) (*SemaphoreTicket, error) {
	now := time.Now()
	s.m.Lock()
	if float64(s.outstanding)+s.debt[p].get(now)+1 < float64(s.capacity) {
		s.outstanding++
		for i := int(p) + 1; i < len(s.debt); i++ {
			s.debt[i].add(now, math.Max(-s.debtForgivePerSuccess, -s.debt[i].get(now)))
		}
		s.m.Unlock()
		return &SemaphoreTicket{parent: s}, nil
	}

	w := newCodelWaiter(now, struct{}{})
	s.queues[p].push(now, w)
	for i := int(p) + 1; i < len(s.debt); i++ {
		s.debt[i].add(now, math.Min(1, float64(s.capacity)-s.debt[i].get(now)))
	}
	s.m.Unlock()

	err := w.wait(ctx)
	if err != nil {
		return nil, err
	}
	return &SemaphoreTicket{parent: s}, nil
}

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

func (s *Semaphore) Close() {
	s.bg.Wait()
}

func (s *Semaphore) admitLocked(now time.Time) {
	for p := range s.queues {
		queue := &s.queues[p]
		for !queue.empty() && float64(s.outstanding)+s.debt[p].get(now)+1 < float64(s.capacity) {
			_, ok := queue.pop(now)
			if ok {
				s.outstanding++
			}
		}
	}
}

func (t *SemaphoreTicket) Release() {
	if t.parent == nil {
		panic("tried to close already closed SemaphoreTicket")
	}

	now := time.Now()

	t.parent.m.Lock()
	t.parent.outstanding--
	t.parent.admitLocked(now)
	t.parent.m.Unlock()

	t.parent = nil
}
