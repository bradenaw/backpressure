package backpressure

import (
	"context"
	"math"
	"time"

	"github.com/bradenaw/juniper/xsync"
)

// RateLimiter is used to bound the rate of some operation. It is a leaky-bucket similar to
// golang.org/x/time/rate, with two major differences:
//
// 1. It is prioritized, preferring to accept higher priority requests first.
//
// 2. Each queue of waiters is a CoDel, which is not fair but can behave better in a real-time
// system under load.
//
// In order to minimize wait times for high-priority requests, it self balances using "debt." Debt
// is tracked per priority and is the number of tokens that must be left in the bucket before a
// given priority may be admitted. For example, if `Priority(1)` has a debt of 5, then a `Wait(ctx,
// Priority(1), 3)` cannot be admitted until there are 8 tokens in the bucket.
//
// Debt is self-adjusting: whenever a high-priority `Wait()` cannot immediately be accepted, the
// debt for all lower priorities is increased. Intuitively, this request would not have had to wait
// if this debt already existed, so the bucket self-corrects by adding it. Whenever a high-priority
// `Acquire()` can be admitted without waiting, then any existing debt may not have been necessary
// and so some of it is forgiven. Additionally, debt decays over time, since anything the bucket has
// learned about a load pattern may become out-of-date as load changes.
type RateLimiter struct {
	bg   *xsync.Group
	debt []expDecay

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

// Additional options for the RateLimiter type. These options do not frequently need to be tuned as
// the defaults work in a majority of cases, but they're included for completeness.
type RateLimiterOption struct{ f func(*rateLimiterOptions) }

type rateLimiterOptions struct {
	shortTimeout          time.Duration
	longTimeout           time.Duration
	debtDecayPctPerSec    float64
	debtForgivePerSuccess float64
}

// The short timeout for the internal CoDels. See the README for more on CoDel.
func RateLimiterShortTimeout(d time.Duration) RateLimiterOption {
	return RateLimiterOption{func(opts *rateLimiterOptions) {
		opts.shortTimeout = d
	}}
}

// The long timeout for the internal CoDels. See the README for more on CoDel.
func RateLimiterLongTimeout(d time.Duration) RateLimiterOption {
	return RateLimiterOption{func(opts *rateLimiterOptions) {
		opts.longTimeout = d
	}}
}

// The percentage by which debt decays per second, in [0, 1]. Debt decays exponentially over time,
// since load patterns change and a previously learned debt amount may no longer be relevant.
func RateLimiterDebtDecayPctPerSec(x float64) RateLimiterOption {
	return RateLimiterOption{func(opts *rateLimiterOptions) {
		opts.debtDecayPctPerSec = x
	}}
}

// The proportion of debt that is forgiven for lower priorities whenever a higher-priority request
// succeeds, in [0, 1].
func RateLimiterDebtForgivePerSuccess(x float64) RateLimiterOption {
	return RateLimiterOption{func(opts *rateLimiterOptions) {
		opts.debtForgivePerSuccess = x
	}}
}

// NewRateLimiter returns a rate limiter with the given number of priorities, allowing the given
// aggregate rate, and up to the given burst. That is, the rate limiter is initially empty and its
// tokens refill at `rate` up to `burst` total tokens.
//
// The other options do not frequently need to be modified.
func NewRateLimiter(
	priorities int,
	rate float64,
	burst float64,
	options ...RateLimiterOption,
) *RateLimiter {
	opts := rateLimiterOptions{
		shortTimeout:          5 * time.Millisecond,
		longTimeout:           100 * time.Millisecond,
		debtDecayPctPerSec:    0.05,
		debtForgivePerSuccess: 0.1,
	}
	for _, option := range options {
		option.f(&opts)
	}

	now := time.Now()
	queues := make([]codel[rlWaiter], priorities)
	debt := make([]expDecay, priorities)
	for i := range queues {
		queues[i] = newCodel[rlWaiter](opts.shortTimeout, opts.longTimeout)
		debt[i] = expDecay{
			decay: opts.debtDecayPctPerSec,
			last:  now,
		}
	}

	rl := &RateLimiter{
		debt:       debt,
		bg:         xsync.NewGroup(context.Background()),
		add:        make(chan *codelWaiter[rlWaiter]),
		rateChange: make(chan rateChange),
	}
	rl.bg.Once(func(ctx context.Context) {
		rl.background(ctx, opts.shortTimeout, rate, burst, opts.debtForgivePerSuccess, queues, now)
	})

	return rl
}

// Wait blocks until the given number of tokens is available for the given priority. It returns nil
// if the tokens were successfully acquired or ErrRejected if the internal CoDel times out the
// request before it can succeed.
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

// Close frees background resources used by the rate limiter.
func (rl *RateLimiter) Close() {
	rl.bg.Wait()
}

func (rl *RateLimiter) background(
	ctx context.Context,
	shortTimeout time.Duration,
	rate float64,
	burst float64,
	debtForgivePerSuccess float64,
	queues []codel[rlWaiter],
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
	tickerC := notReady

	timerRunning := false

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
		for i := range queues {
			queues[i].reap(now)
		}

		refill(now)

		// Look through the queues from high priority to low priority to find somebody that's ready
		// to wake. Because we always prefer to admit higher priorities, if we find anybody that
		// isn't ready to wake yet, then they're the next one to be admitted and we can set the
		// timer accordingly.
		for p := range queues {
			queue := &queues[p]
			for {
				item, ok := queue.next()
				if !ok {
					// No high-priority waiters, check the next priority down.
					break
				}
				need := item.tokens - (tokens - rl.debt[p].get(now))
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
						if !nextTimer.Stop() && timerRunning {
							<-nextTimer.C
						}
						nextTimer.Reset(nextReady)
					}
					timerRunning = true
					ready = nextTimer.C
					// There's already a waiter for a higher priority, so don't bother even looking
					// at the lower-priority queues.
					return
				}
			}
		}
		// If we're here, all of the queues are empty.
		ready = notReady
		tickerC = notReady
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
			hasHigherPriority := false
			for p := 0; p < int(w.t.p); p++ {
				hasHigherPriority = hasHigherPriority || !queues[p].empty()
			}
			need := w.t.tokens - (tokens - rl.debt[int(w.t.p)].get(now))
			// See if we can admit them right away.
			if !hasHigherPriority && need <= 0 {
				ok := w.admit()
				if ok {
					tokens -= w.t.tokens
				}
				for i := int(w.t.p) + 1; i < len(queues); i++ {
					rl.debt[i].add(now, math.Max(-rl.debt[i].get(now), need*debtForgivePerSuccess))
				}
			} else {
				// Otherwise, this waiter needs to block for a while. Penalize the lower priority
				// queues to try to make sure waiters of this priority don't need to block in the
				// future.
				for i := int(w.t.p) + 1; i < len(queues); i++ {
					rl.debt[i].add(now, math.Min(burst-rl.debt[i].get(now), need))
				}
				queues[int(w.t.p)].push(now, w)
				admit(now)
			}
			queues[int(w.t.p)].push(now, w)
			tickerC = ticker.C
			admit(now)
		case rc := <-rl.rateChange:
			rate = rc.rate
			burst = rc.burst
			admit(time.Now())
		case <-tickerC:
			// Ticker used to make sure we're reaping regularly, which might mean adjusting the
			// timeout of the codels. admit handles that and setting `ready` properly.
			admit(time.Now())
		case <-ready:
			// Timer fired, somebody is ready to be admitted. Probably. Might've tripped over codel
			// lifetime but admit handles that.
			timerRunning = false
			admit(time.Now())
		}
	}
}
