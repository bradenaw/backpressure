package backpressure

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"
)

const forever = 365 * 24 * time.Hour

// RateLimiter is used to bound the rate of some operation. It is a token-bucket similar to
// golang.org/x/time/rate. Conceptually, it is a bucket of some capacity (the "burst"), which
// refills at some rate. Callers ask for some number of tokens from the bucket using Wait, they take
// the tokens from the bucket and Wait returns. If there are not enough tokens, then Wait blocks to
// wait for enough tokens to be replenished.
//
// It has two major differences from golang.org/x/time/rate:
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
	longTimeout           time.Duration
	debtForgivePerSuccess float64

	// All below protected by m.
	m      sync.Mutex
	bgDone chan struct{}
	// Set to longTimeout if there are any waiters, or forever otherwise. Nil if the rate limiter
	// has already been closed.
	reapTicker *time.Ticker
	// Set to the time when the next waiter can be admitted if there are any, otherwise set to
	// forever.
	nextReady *time.Timer
	// The timestamp of the last refill that updated tokens.
	lastFill time.Time
	// The number of tokens in the rate limiter as of lastFill.
	tokens float64
	// The refill rate in tokens per second.
	rate float64
	// The maximum number of tokens that can be in `tokens`.
	burst float64
	// Are there any waiters in queues?
	hasWaiters bool
	// Queues by priority.
	queues []codel[rlWaiter]
	// Debt by priority.
	debt []expDecay
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
		longTimeout:           opts.longTimeout,
		debtForgivePerSuccess: opts.debtForgivePerSuccess,

		reapTicker: time.NewTicker(forever),
		nextReady:  time.NewTimer(forever),
		lastFill:   time.Now(),
		tokens:     0,
		rate:       rate,
		burst:      burst,
		queues:     queues,
		debt:       debt,
	}

	return rl
}

// Wait blocks until the given number of tokens is available for the given priority. It returns nil
// if the tokens were successfully acquired or ErrRejected if the internal CoDel times out the
// request before it can succeed.
func (rl *RateLimiter) Wait(ctx context.Context, p Priority, tokens float64) error {
	rl.m.Lock()

	if rl.reapTicker == nil {
		rl.m.Unlock()
		return errAlreadyClosed
	}
	if math.IsInf(rl.rate, 1) {
		rl.m.Unlock()
		return nil
	}
	if rl.rate == 0 {
		rl.m.Unlock()
		return errZeroRate
	}

	if tokens > rl.burst {
		rl.m.Unlock()
		return fmt.Errorf(
			"tried to Wait for %f tokens, rate limiter's max burst is %f",
			tokens,
			rl.burst,
		)
	}

	now := time.Now()
	rl.refill(now)

	hasHigherPriority := false
	for i := 0; i < int(p)+1; i++ {
		if !rl.queues[i].empty() {
			hasHigherPriority = true
			break
		}
	}

	need := tokens - (rl.tokens - rl.debt[int(p)].get(now))
	// See if we can admit right away.
	if !hasHigherPriority && need <= 0 {
		rl.tokens -= tokens
		for i := int(p) + 1; i < len(rl.queues); i++ {
			rl.debt[i].add(now, math.Max(-rl.debt[i].get(now), need*rl.debtForgivePerSuccess))
		}
		rl.m.Unlock()
		return nil
	}

	// Otherwise, this waiter needs to block for a while. Penalize the lower priority
	// queues to try to make sure waiters of this priority don't need to block in the
	// future.
	for i := int(p) + 1; i < len(rl.queues); i++ {
		rl.debt[i].add(now, math.Min(rl.burst-rl.debt[i].get(now), need))
	}

	w := newCodelWaiter(time.Now(), rlWaiter{
		p:      p,
		tokens: tokens,
	})
	rl.queues[int(p)].push(now, w)
	if !rl.hasWaiters {
		rl.hasWaiters = true
		rl.reapTicker.Reset(rl.longTimeout)
	}
	rl.admit(now)
	if rl.bgDone == nil {
		rl.bgDone = make(chan struct{})
		go rl.background()
	}

	rl.m.Unlock()

	return w.wait(ctx)
}

func (rl *RateLimiter) SetRate(rate float64, burst float64) {
	if rate < 0 {
		panic("negative rate")
	}
	if burst < 0 {
		panic("negative burst")
	}
	rl.m.Lock()
	if rl.reapTicker == nil {
		rl.m.Unlock()
		return
	}
	now := time.Now()
	rl.refill(now)
	rl.rate = rate
	rl.burst = burst
	if rl.tokens > burst {
		rl.tokens = burst
	}
	rl.admit(now)
	rl.m.Unlock()
}

// Close frees background resources used by the rate limiter.
func (rl *RateLimiter) Close() {
	rl.m.Lock()
	rl.nextReady.Stop()
	rl.reapTicker.Stop()
	// Signal to further Wait()s.
	rl.reapTicker = nil
	if rl.bgDone != nil {
		close(rl.bgDone)
	}
	rl.m.Unlock()
}

func (rl *RateLimiter) refill(now time.Time) {
	rl.tokens += now.Sub(rl.lastFill).Seconds() * rl.rate
	if rl.tokens > rl.burst {
		rl.tokens = rl.burst
	}
	rl.lastFill = now
}

func (rl *RateLimiter) admit(now time.Time) {
	for i := range rl.queues {
		rl.queues[i].reap(now)
	}

	// Look through the queues from high priority to low priority to find somebody that's ready to
	// wake. Because we always prefer to admit higher priorities, if we find anybody that isn't
	// ready to wake yet, then they're the next one to be admitted and we can set the timer
	// accordingly.
	for p := range rl.queues {
		queue := &rl.queues[p]
		for {
			item, ok := queue.next()
			if !ok {
				// No high-priority waiters, check the next priority down.
				break
			}
			need := item.tokens - (rl.tokens - rl.debt[p].get(now))
			if need <= 0 {
				// We can fill their request right now.
				item, ok := queue.pop(now)
				if !ok {
					continue
				}
				rl.tokens -= item.tokens
			} else {
				timeToNext := time.Duration(need / rl.rate * float64(time.Second))
				rl.nextReady.Stop()
				rl.nextReady.Reset(timeToNext)
				// There's already a waiter for a higher priority, so don't bother even looking
				// at the lower-priority queues.
				return
			}
		}
	}

	// All queues are empty.
	if rl.hasWaiters {
		rl.reapTicker.Reset(forever)
		rl.nextReady.Reset(forever)
		rl.hasWaiters = false
	}
}

func (rl *RateLimiter) background() {
	for {
		select {
		case <-rl.bgDone:
			return
		case <-rl.reapTicker.C:
			rl.m.Lock()
			if rl.reapTicker == nil {
				rl.m.Unlock()
				return
			}
			rl.admit(time.Now())
			rl.m.Unlock()
		case <-rl.nextReady.C:
			rl.m.Lock()
			if rl.reapTicker == nil {
				rl.m.Unlock()
				return
			}
			rl.admit(time.Now())
			rl.m.Unlock()
		}
	}
}
