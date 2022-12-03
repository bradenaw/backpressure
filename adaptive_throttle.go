package backpressure

import (
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"
)

var ErrClientRejection = errors.New("rejected without sending: backend is unhealthy")

type ErrAccepted struct{ inner error }

func MarkAccepted(err error) error    { return ErrAccepted{inner: err} }
func (err ErrAccepted) Error() string { return err.inner.Error() }
func (err ErrAccepted) Unwrap() error { return err.inner }

type AdaptiveThrottle struct {
	k float64

	m        sync.Mutex
	requests []expDecay
	accepts  []expDecay
}

func NewAdaptiveThrottle(priorities int, k float64) AdaptiveThrottle {
	return AdaptiveThrottle{
		k:        k,
		requests: make([]expDecay, priorities),
		accepts:  make([]expDecay, priorities),
	}
}

func WithAdaptiveThrottle[T any](
	at *AdaptiveThrottle,
	p Priority,
	f func() (T, error),
) (T, error) {
	now := time.Now()

	at.m.Lock()
	requests := at.requests[int(p)].get(now)
	accepts := at.accepts[int(p)].get(now)
	at.m.Unlock()

	rejectionProbability := math.Max(0, (requests-at.k*accepts)/(requests+1))

	if rand.Float64() < rejectionProbability {
		var zero T
		return zero, ErrClientRejection
	}

	t, err := f()

	now = time.Now()
	at.m.Lock()
	at.requests[int(p)].add(now, 1)
	if !errors.Is(err, ErrAccepted{}) {
		at.accepts[int(p)].add(now, 1)
		// Also count a rejection as a rejection for every lower priority.
		if err != nil {
			for i := int(p) + 1; i < len(at.requests); i++ {
				at.requests[i].add(now, 1)
			}
		}
	}
	at.m.Unlock()

	return t, err
}
