package backpressure

import (
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"
)

// MarkAccepted marks the given err as "accepted" by the backend for the purposes of
// AdaptiveThrottle. This should be done for regular protocol errors that do not mean the backend is
// unhealthy.
func MarkAccepted(err error) error { return errAccepted{inner: err} }

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
	k float64

	m        sync.Mutex
	requests []windowedCounter
	accepts  []windowedCounter
}

// NewAdaptiveThrottle returns an AdaptiveThrottle.
//
// priorities is the number of priorities that the throttle will accept. Giving a priority outside
// of `[0, priorities)` will panic.
//
// k is the ratio of the measured success rate and the rate that the throttle will admit. For
// example, when k is 2 the throttle will allow twice as many requests to actually reach the backend
// as it believes will succeed. Higher values of k mean that the throttle will react more slowly
// when a backend becomes unhealthy, but react more quickly when it becomes healthy again, and will
// allow more load to an unhealthy backend. k=2 is usually a good place to start, but backends that
// serve "cheap" requests (e.g. in-memory caches) may need a lower value.
//
// d is the time window over which the throttle remembers requests for use in figuring out the
// success rate.
func NewAdaptiveThrottle(priorities int, k float64, d time.Duration) *AdaptiveThrottle {
	now := time.Now()
	requests := make([]windowedCounter, priorities)
	accepts := make([]windowedCounter, priorities)
	for i := range requests {
		requests[i] = newWindowedCounter(now, d/10, 10)
		accepts[i] = newWindowedCounter(now, d/10, 10)
	}

	return &AdaptiveThrottle{
		k:        k,
		requests: requests,
		accepts:  accepts,
	}
}

// WithAdaptiveThrottle is used to send a request to a backend using the given AdaptiveThrottle for
// client-rejections.
func WithAdaptiveThrottle[T any](
	at *AdaptiveThrottle,
	p Priority,
	f func() (T, error),
) (T, error) {
	now := time.Now()

	at.m.Lock()
	requests := float64(at.requests[int(p)].get(now))
	accepts := float64(at.accepts[int(p)].get(now))
	at.m.Unlock()

	rejectionProbability := math.Max(0, (requests-at.k*accepts)/(requests+1))

	if rand.Float64() < rejectionProbability {
		var zero T
		at.m.Lock()
		at.requests[int(p)].add(now, 1)
		at.m.Unlock()
		return zero, ErrClientRejection
	}

	t, err := f()

	now = time.Now()
	at.m.Lock()
	at.requests[int(p)].add(now, 1)
	if !errors.Is(err, errAccepted{}) {
		at.accepts[int(p)].add(now, 1)
		// Also count a rejection as a rejection for every lower priority.
		//
		// TODO: Is this a good idea? We have no way of informing the lower priorities that things
		// are good again, they have to figure it out on their own.
		if err != nil {
			for i := int(p) + 1; i < len(at.requests); i++ {
				at.requests[i].add(now, 1)
			}
		}
	}
	at.m.Unlock()

	return t, err
}
