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
	k float32

	m        sync.Mutex
	requests [nPriorities]expDecay
	accepts  [nPriorities]expDecay
}

func NewAdaptiveThrottle(k float32) AdaptiveThrottle {
	return AdaptiveThrottle{k: k}
}

func WithAdaptiveThrottle[T any](
	ctx context.Context,
	at *AdaptiveThrottle,
	f func() (T, error),
) (T, error) {
	p, _ := ContextPriority(ctx)
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
	if !errors.Is(err, ErrAccepted) {
		at.accepts[int(p)].add(now, 1)
		// Also count a rejection as a rejection for every lower priority.
		if err != nil {
			for i := int(p) + 1; i < len(at.requests); i++ {
				at.requests.add(now, 1)
			}
		}
	}
	at.m.Unlock()

	return t, err
}
