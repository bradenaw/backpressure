package backpressure

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/bradenaw/juniper/xsync"
)

type Semaphore struct {
	shortTimeout time.Duration
	bg           *xsync.Group

	m           sync.Mutex
	capacity    int
	outstanding int
	queues      []codel[struct{}]
	debt        []expDecay
}

type SemaphoreTicket struct {
	parent *Semaphore
}

func NewSemaphore(
	priorities int,
	shortTimeout time.Duration,
	longTimeout time.Duration,
	capacity int,
	debtDecay float64,
) *Semaphore {
	now := time.Now()

	s := &Semaphore{
		capacity:     capacity,
		shortTimeout: shortTimeout,
		bg:           xsync.NewGroup(context.Background()),
		queues:       make([]codel[struct{}], priorities),
		debt:         make([]expDecay, priorities),

		outstanding: 0,
	}

	for i := range s.queues {
		s.queues[i] = newCodel[struct{}](shortTimeout, longTimeout)
		s.debt[i] = expDecay{
			decay: debtDecay,
			last:  now,
		}
	}
	s.bg.Once(s.background)

	return s
}

func (s *Semaphore) Admit(ctx context.Context, p Priority) (*SemaphoreTicket, error) {
	now := time.Now()
	s.m.Lock()
	if float64(s.outstanding)+s.debt[p].get(now) < float64(s.capacity) {
		s.outstanding++
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
			for _, queue := range s.queues {
				queue.reap(now)
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
	for p, queue := range s.queues {
		for !queue.empty() {
			if float64(s.outstanding)+s.debt[p].get(now) < float64(s.capacity) {
				_, ok := s.queues[p].pop(now)
				if ok {
					s.outstanding++
				}
			}
		}
	}
}

func (t *SemaphoreTicket) Close() {
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
