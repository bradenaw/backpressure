package backpressure

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/bradenaw/juniper/xsync"
)

type Semaphore struct {
	capacity     int
	shortTimeout time.Duration
	bg           *xsync.Group

	m         sync.Mutex
	available int
	queues    []codel[struct{}]
	debt      []expDecay
}

type SemaphoreTicket struct {
	parent *Semaphore
}

func NewSemaphore(
	shortTimeout time.Duration,
	longTimeout time.Duration,
	capacity int,
	debtDecay float64,
) *Semaphore {
	now := time.Now()
	queues := make([]codel[struct{}], nPriorities)
	debt := make([]expDecay, nPriorities)
	for i := range queues {
		queues[i] = newCodel[struct{}](shortTimeout, longTimeout)
		debt[i] = expDecay{
			decay: debtDecay,
			last:  now,
		}
	}

	s := &Semaphore{
		capacity:     capacity,
		shortTimeout: shortTimeout,
		bg:           xsync.NewGroup(context.Background()),

		available: capacity,
		queues:    queues,
		debt:      debt,
	}
	s.bg.Once(s.background)

	return s
}

func (s *Semaphore) Admit(ctx context.Context, p Priority) (*SemaphoreTicket, error) {
	now := time.Now()
	s.m.Lock()
	if float64(s.available)-s.debt[p].get(now) >= 1 {
		s.available--
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
			for _, queue := range s.queues {
				queue.reap(now)
			}
			s.maybeAdmitLocked(now)
			s.m.Unlock()
		}
	}
}

func (s *Semaphore) Close() {
	s.bg.Wait()
}

func (s *Semaphore) maybeAdmitLocked(now time.Time) {
	for p, queue := range s.queues {
		if queue.empty() {
			continue
		}
		if float64(s.available)-s.debt[p].get(now) >= 1 {
			s.available--
			s.queues[p].pop(now)
		}
	}
}

func (t *SemaphoreTicket) Close() {
	if t.parent == nil {
		panic("tried to close already closed SemaphoreTicket")
	}

	now := time.Now()

	t.parent.m.Lock()
	t.parent.available++
	t.parent.maybeAdmitLocked(now)
	t.parent.m.Unlock()

	t.parent = nil
}
