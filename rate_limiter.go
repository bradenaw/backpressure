package load_mgmt

import (
	"context"
	"errors"
	"math"
	"sync/atomic"
	"time"

	"github.com/bradenaw/juniper/container/deque"
	"github.com/bradenaw/juniper/xsync"
)

var (
	ErrTimedOut = errors.New("timed out")
)

type RateLimiter struct {
	shortTimeout time.Duration
	tokensPerSec float64
	burst        float64
	bg           *xsync.Group

	// TODO: track demand per priority and require padding before admitting lower priorities so that
	// high priorities do not have to wait
	add      chan *rlWaiter
	queues   []codel[*rlWaiter]
	debt     []expDecay
	tokens   float64
	lastFill time.Time
}

func NewRateLimiter(
	shortTimeout time.Duration,
	longTimeout time.Duration,
	tokensPerSec float64,
	burst float64,
	debtDecay float64,
) *RateLimiter {
	now := time.Now()
	queues := make([]codel[*rlWaiter], nPriorities)
	debt := make([]expDecay, nPriorities)
	for i := range queues {
		queues[i] = newCodel[*rlWaiter](shortTimeout, longTimeout)
		debt[i] = expDecay{
			decay: debtDecay,
			last:  now,
		}
	}

	rl := &RateLimiter{
		shortTimeout: shortTimeout,
		tokensPerSec: tokensPerSec,
		burst:        burst,
		bg:           xsync.NewGroup(context.Background()),

		add:      make(chan *rlWaiter),
		queues:   queues,
		debt:     debt,
		tokens:   0,
		lastFill: now,
	}
	rl.bg.Once(rl.background)

	return rl
}

func (rl *RateLimiter) Wait(ctx context.Context, p Priority, tokens float64) error {
	w := &rlWaiter{
		p:      p,
		tokens: tokens,
		c:      make(chan bool, 1),
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case rl.add <- w:
	}
	return w.wait(ctx)
}

func (rl *RateLimiter) Close() {
	rl.bg.Wait()
}

func (rl *RateLimiter) refill(now time.Time) {
	rl.tokens += now.Sub(rl.lastFill).Seconds() * rl.tokensPerSec
	if rl.tokens > rl.burst {
		rl.tokens = rl.burst
	}
	rl.lastFill = now
}

func (rl *RateLimiter) background(ctx context.Context) {
	ticker := time.NewTicker(rl.shortTimeout / 2)
	var nextTimer *time.Timer
	var notReady <-chan time.Time
	ready := notReady

	admit := func(now time.Time) {
		// Reap all of the queues, which may flip them to short timeout + LIFO, which might change
		// who's next.
		for _, queue := range rl.queues {
			queue.reap(now)
		}
		rl.refill(now)
		for p, queue := range rl.queues {
			for {
				item, ok := queue.next()
				if !ok {
					// No high-priority waiters, check the next priority down.
					break
				}
				need := rl.tokens + rl.debt[p].get(now) - item.tokens
				if need <= 0 {
					// We can fill their request right now.
					queue.pop(now)
					ok := item.admit()
					if !ok {
						continue
					}
					rl.tokens -= item.tokens
				} else {
					for i := p + 1; i < len(rl.queues); i++ {
						rl.debt[i].add(now, math.Min(rl.debt[i].get(now), need))
					}
					nextReady := time.Duration(need / rl.tokensPerSec * float64(time.Second))
					if nextTimer == nil {
						nextTimer = time.NewTimer(nextReady)
					} else {
						if !nextTimer.Stop() {
							<-nextTimer.C
						}
						nextTimer.Reset(nextReady)
					}
					ready = nextTimer.C
					// There's already a waiter for a higher priority, so don't bother even looking
					// at the lower-priority queues.
					return
				}
			}
		}
		if nextTimer != nil {
			if !nextTimer.Stop() {
				<-nextTimer.C
			}
		}
		// If we're here, all of the queues are empty.
		ready = notReady
	}

	for {
		select {
		case w := <-rl.add:
			now := time.Now()
			rl.queues[int(w.p)].push(now, w)
			// We might be able to admit them right away, so give it a go.
			admit(now)
		case <-ticker.C:
			// Ticker used to make sure we're reaping regularly, which might mean adjusting the
			// timeout of the codels. admit handles that and setting `ready` properly.
			admit(time.Now())
		case <-ready:
			// Timer fired, somebody is ready to be admitted. Probably. Might've tripped over codel
			// lifetime but admit handles that.
			admit(time.Now())
		}
	}
}

type rlWaiter struct {
	p      Priority
	tokens float64
	s      uint32
	c      chan bool
}

func (w *rlWaiter) wait(ctx context.Context) error {
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

func (w *rlWaiter) admit() bool {
	ok := atomic.CompareAndSwapUint32(&w.s, 0, 1)
	w.c <- true
	return ok
}

func (w *rlWaiter) drop() {
	w.c <- false
}

type codelMode int

const (
	codelModeFIFO codelMode = iota
	codelModeLIFO
)

type codelItem[T codelT] struct {
	enqueued time.Time
	t        T
}

type codelT interface {
	drop()
}

type codel[T codelT] struct {
	shortTimeout time.Duration
	longTimeout  time.Duration
	lastEmpty    time.Time
	mode         codelMode
	items        deque.Deque[codelItem[T]]
}

func newCodel[T codelT](shortTimeout time.Duration, longTimeout time.Duration) codel[T] {
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
	var item T
	switch c.mode {
	case codelModeFIFO:
		item = c.items.PopFront().t
	case codelModeLIFO:
		item = c.items.PopBack().t
	default:
		panic("unreachable")
	}
	c.setMode(now)
	return item, true
}

func (c *codel[T]) push(now time.Time, t T) {
	c.items.PushBack(codelItem[T]{
		enqueued: now,
		t:        t,
	})
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
		item.t.drop()
	}
	c.setMode(now)
}

type expDecay struct {
	decay float64
	last  time.Time
	ctr   float64
}

func (d *expDecay) add(now time.Time, x float64) {
	d.get(now)
	d.ctr += x
}

func (d *expDecay) get(now time.Time) float64 {
	d.ctr *= math.Pow((1 - d.decay), now.Sub(d.last).Seconds())
	d.last = now
	return d.ctr
}
