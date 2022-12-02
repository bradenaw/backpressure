package load_mgmt

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/bradenaw/juniper/xsync"
)

var (
	ErrTimedOut = errors.New("timed out")
)

type RateLimiter struct {
	shortTimeout time.Duration
	rate         float64
	burst        float64
	bg           *xsync.Group

	// TODO: track demand per priority and require padding before admitting lower priorities so that
	// high priorities do not have to wait
	add      chan *codelWaiter[rlWaiter]
	queues   []codel[rlWaiter]
	debt     []expDecay
	tokens   float64
	lastFill time.Time
}

type rlWaiter struct {
	p      Priority
	tokens float64
}

func NewRateLimiter(
	shortTimeout time.Duration,
	longTimeout time.Duration,
	rate float64,
	burst float64,
	debtDecay float64,
) *RateLimiter {
	now := time.Now()
	queues := make([]codel[rlWaiter], nPriorities)
	debt := make([]expDecay, nPriorities)
	for i := range queues {
		queues[i] = newCodel[rlWaiter](shortTimeout, longTimeout)
		debt[i] = expDecay{
			decay: debtDecay,
			last:  now,
		}
	}

	rl := &RateLimiter{
		shortTimeout: shortTimeout,
		rate:         rate,
		burst:        burst,
		bg:           xsync.NewGroup(context.Background()),

		add:      make(chan *codelWaiter[rlWaiter]),
		queues:   queues,
		debt:     debt,
		tokens:   0,
		lastFill: now,
	}
	rl.bg.Once(rl.background)

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

func (rl *RateLimiter) Close() {
	rl.bg.Wait()
}

func (rl *RateLimiter) refill(now time.Time) {
	rl.tokens += now.Sub(rl.lastFill).Seconds() * rl.rate
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
					item, ok := queue.pop(now)
					if !ok {
						continue
					}
					rl.tokens -= item.tokens
				} else {
					for i := p + 1; i < len(rl.queues); i++ {
						rl.debt[i].add(now, math.Min(rl.burst-rl.debt[i].get(now), need))
					}
					nextReady := time.Duration(need / rl.rate * float64(time.Second))
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
			rl.queues[int(w.t.p)].push(now, w)
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
