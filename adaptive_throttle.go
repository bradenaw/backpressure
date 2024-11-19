package backpressure

import (
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"
)

// AcceptedError wraps the given err as "accepted" by the backend for the purposes of
// AdaptiveThrottle. This should be done for regular protocol errors that do not mean the backend is
// unhealthy, for example a precondition failure as part of an optimistic update or a rejection for
// a particular request being too large.
func AcceptedError(err error) error { return errAccepted{inner: err} }

type errAccepted struct{ inner error }

func (err errAccepted) Error() string { return err.inner.Error() }
func (err errAccepted) Unwrap() error { return err.inner }

// AdaptiveThrottle is used in a client to throttle requests to a backend as it becomes unhealthy to
// help it recover from overload more quickly. Because backends must expend resources to reject
// requests over their capacity it is vital for clients to ease off on sending load when they are
// in trouble, lest the backend spend all of its resources on rejecting requests and have none left
// over to actually serve any.
//
// The adaptive throttle works by tracking the success rate of requests over some time interval
// (usually a minute or so), and randomly rejecting requests without sending them to avoid sending
// too much more than the rate that are expected to actually be successful. Some slop is included,
// because even if the backend is serving zero requests successfully, we do need to occasionally
// send it requests to learn when it becomes healthy again.
//
// More on adaptive throttles in https://sre.google/sre-book/handling-overload/
type AdaptiveThrottle struct {
	k            float64
	minPerWindow float64

	m        sync.Mutex
	requests []windowedCounter
	accepts  []windowedCounter
}

// Additional options for the AdaptiveThrottle type. These options do not frequently need to be
// tuned as the defaults work in a majority of cases.
type AdaptiveThrottleOption struct {
	f func(*adaptiveThrottleOptions)
}

type adaptiveThrottleOptions struct {
	k       float64
	minRate float64
	d       time.Duration
}

// AdaptiveThrottleRatio sets the ratio of the measured success rate and the rate that the throttle
// will admit. For example, when k is 2 the throttle will allow twice as many requests to actually
// reach the backend as it believes will succeed. Higher values of k mean that the throttle will
// react more slowly when a backend becomes unhealthy, but react more quickly when it becomes
// healthy again, and will allow more load to an unhealthy backend. k=2 is usually a good place to
// start, but backends that serve "cheap" requests (e.g. in-memory caches) may need a lower value.
func AdaptiveThrottleRatio(k float64) AdaptiveThrottleOption {
	return AdaptiveThrottleOption{func(opts *adaptiveThrottleOptions) {
		opts.k = k
	}}
}

// AdaptiveThrottleMinimumRate sets the minimum number of requests per second that the adaptive
// throttle will allow (approximately) through to the upstream, even if every request is failing.
// This is important because this is how the adaptive throttle 'learns' when the upstream becomes
// healthy again.
func AdaptiveThrottleMinimumRate(x float64) AdaptiveThrottleOption {
	return AdaptiveThrottleOption{func(opts *adaptiveThrottleOptions) {
		opts.minRate = x
	}}
}

// AdaptiveThrottleWindow sets the time window over which the throttle remembers requests for use in
// figuring out the success rate.
func AdaptiveThrottleWindow(d time.Duration) AdaptiveThrottleOption {
	return AdaptiveThrottleOption{func(opts *adaptiveThrottleOptions) {
		opts.d = d
	}}
}

// NewAdaptiveThrottle returns an AdaptiveThrottle.
//
// priorities is the number of priorities that the throttle will accept. Giving a priority outside
// of `[0, priorities)` will panic.
func NewAdaptiveThrottle(priorities int, options ...AdaptiveThrottleOption) *AdaptiveThrottle {
	opts := adaptiveThrottleOptions{
		d:       time.Minute,
		k:       2,
		minRate: 1,
	}
	for _, option := range options {
		option.f(&opts)
	}

	now := time.Now()
	requests := make([]windowedCounter, priorities)
	accepts := make([]windowedCounter, priorities)
	for i := range requests {
		requests[i] = newWindowedCounter(now, opts.d/10, 10)
		accepts[i] = newWindowedCounter(now, opts.d/10, 10)
	}

	return &AdaptiveThrottle{
		k:            opts.k,
		requests:     requests,
		accepts:      accepts,
		minPerWindow: opts.minRate * opts.d.Seconds(),
	}
}

// Attempt returns true if the request should be attempted, and false if it should be rejected.
func (at *AdaptiveThrottle) Attempt(
	p Priority,
) bool {
	now := time.Now()
	// Lifted rather directly from https://sre.google/sre-book/handling-overload/, with two
	// extensions:
	// - We count higher priorities' non-accepts as non-accepts, since we're trying to estimate
	//   roughly how many requests we can send through without causing rejections for higher
	//   priorities.
	// - minPerWindow is configurable, in the book it's always 1 meaning ~1 QPS is the minimum
	//   allowed.
	at.m.Lock()
	requests := float64(at.requests[int(p)].get(now))
	accepts := float64(at.accepts[int(p)].get(now))
	for i := 0; i < int(p); i++ {
		// Also count non-accepted requests for every higher priority as non-accepted for this
		// priority.
		requests += float64(at.requests[i].get(now) - at.accepts[i].get(now))
	}
	at.m.Unlock()

	rejectionProbability := math.Max(0, (requests-at.k*accepts)/(requests+at.minPerWindow))

	if rand.Float64() < rejectionProbability {
		at.m.Lock()
		at.requests[int(p)].add(now, 1)
		at.m.Unlock()
		return false
	}
	return true
}

// Accepted records that a request was accepted by the backend.
func (at *AdaptiveThrottle) Accepted(p Priority) {
	now := time.Now()
	at.m.Lock()
	at.requests[int(p)].add(now, 1)
	at.accepts[int(p)].add(now, 1)
	at.m.Unlock()
}

// Rejected records that a request was rejected by the backend.
func (at *AdaptiveThrottle) Rejected(p Priority) {
	now := time.Now()
	at.m.Lock()
	at.requests[int(p)].add(now, 1)
	at.m.Unlock()
}

// WithAdaptiveThrottle is used to send a request to a backend using the given AdaptiveThrottle for
// client-rejections.
//
// If f returns an error, at considers this to be a rejection unless it is wrapped with
// AcceptedError(). If there are enough rejections within a given time window, further calls to
// WithAdaptiveThrottle may begin returning ErrClientRejection immediately without invoking f. The
// rate at which this happens depends on the error rate of f.
//
// WithAdaptiveThrottle will prefer to reject lower-priority requests if it can.
func WithAdaptiveThrottle[T any](
	at *AdaptiveThrottle,
	p Priority,
	f func() (T, error),
) (T, error) {
	ok := at.Attempt(p)
	if !ok {
		var zero T
		return zero, ErrClientRejection
	}

	t, err := f()

	if err == nil || errors.Is(err, errAccepted{}) {
		at.Accepted(p)
	} else {
		at.Rejected(p)
	}

	return t, err
}
