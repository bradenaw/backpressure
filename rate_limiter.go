package backpressure

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/bradenaw/juniper/xsync"
)

var (
	ErrRejected = errors.New("rejected")
)

type RateLimiter struct {
	bg *xsync.Group

	add        chan *codelWaiter[rlWaiter]
	rateChange chan rateChange
}

type rlWaiter struct {
	p      Priority
	tokens float64
}

type rateChange struct {
	rate  float64
	burst float64
	c     chan struct{}
}

func NewRateLimiter(
	priorities int,
	shortTimeout time.Duration,
	longTimeout time.Duration,
	rate float64,
	burst float64,
	debtDecay float64,
) *RateLimiter {
	now := time.Now()
	queues := make([]codel[rlWaiter], priorities)
	debt := make([]expDecay, priorities)
	for i := range queues {
		queues[i] = newCodel[rlWaiter](shortTimeout, longTimeout)
		debt[i] = expDecay{
			decay: debtDecay,
			last:  now,
		}
	}

	rl := &RateLimiter{
		bg:         xsync.NewGroup(context.Background()),
		add:        make(chan *codelWaiter[rlWaiter]),
		rateChange: make(chan rateChange),
	}
	rl.bg.Once(func(ctx context.Context) {
		rl.background(ctx, shortTimeout, rate, burst, queues, debt, now)
	})

	return rl
}

func (rl *RateLimiter) Wait(ctx context.Context, p Priority, tokens float64) error {
	w := newCodelWaiter(time.Now(), rlWaiter{
		p:      p,
		tokens: tokens,
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case rl.add <- w:
	}
	return w.wait(ctx)
}

func (rl *RateLimiter) SetRate(rate float64, burst float64) {
	c := make(chan struct{})
	rl.rateChange <- rateChange{
		rate:  rate,
		burst: burst,
		c:     c,
	}
	<-c
}

func (rl *RateLimiter) Close() {
	rl.bg.Wait()
}

func (rl *RateLimiter) background(
	ctx context.Context,
	shortTimeout time.Duration,
	rate float64,
	burst float64,
	queues []codel[rlWaiter],
	debt []expDecay,
	start time.Time,
) {
	tokens := float64(0)
	lastFill := start

	// Ticker needs to fire every so often to reap the codels.
	ticker := time.NewTicker(shortTimeout / 2)
	// nextTimer is the timer until the next waiter can be admitted, if there is one. It's nil until
	// the first waiter, after that never nil, we always reset.
	var nextTimer *time.Timer
	// notReady is a nil channel which blocks forever on receive.
	var notReady <-chan time.Time
	// ready is delivered to when the next waiter is (probably) ready to be admitted.
	ready := notReady

	refill := func(now time.Time) {
		// Refill tokens.
		tokens += now.Sub(lastFill).Seconds() * rate
		if tokens > burst {
			tokens = burst
		}
		lastFill = now
	}

	admit := func(now time.Time) {
		// Reap all of the queues, which may flip them to short timeout + LIFO, which might change
		// who's next.
		for _, queue := range queues {
			queue.reap(now)
		}

		refill(now)

		// Look through the queues from high priority to low priority to find somebody that's ready
		// to wake. Because we always prefer to admit higher priorities, if we find anybody that
		// isn't ready to wake yet, then they're the next one to be admitted and we can set the
		// timer accordingly.
		for p, queue := range queues {
			for {
				item, ok := queue.next()
				if !ok {
					// No high-priority waiters, check the next priority down.
					break
				}
				need := tokens + debt[p].get(now) - item.tokens
				if need <= 0 {
					// We can fill their request right now.
					item, ok := queue.pop(now)
					if !ok {
						continue
					}
					tokens -= item.tokens
				} else {
					nextReady := time.Duration(need / rate * float64(time.Second))
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
		case <-ctx.Done():
			ticker.Stop()
			if nextTimer != nil {
				nextTimer.Stop()
			}
			return
		case w := <-rl.add:
			now := time.Now()
			refill(now)
			need := tokens + debt[int(w.t.p)].get(now) - w.t.tokens
			// See if we can admit them right away.
			if need <= 0 {
				ok := w.admit()
				if ok {
					tokens -= w.t.tokens
				}
			} else {
				// Otherwise, this waiter needs to block for a while. Penalize the lower priority
				// queues to try to make sure waiters of this priority don't need to block in the
				// future.
				for i := int(w.t.p) + 1; i < len(queues); i++ {
					debt[i].add(now, math.Min(burst-debt[i].get(now), need))
				}
				queues[int(w.t.p)].push(now, w)
				admit(now)
			}
		case rc := <-rl.rateChange:
			rate = rc.rate
			burst = rc.burst
			admit(time.Now())
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
