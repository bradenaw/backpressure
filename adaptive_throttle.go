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
	requests []timeBucketedCounter
	accepts  []timeBucketedCounter
}

func NewAdaptiveThrottle(priorities int, k float64, d time.Duration) AdaptiveThrottle {
	now := time.Now()
	requests := make([]timeBucketedCounter, priorities)
	accepts := make([]timeBucketedCounter, priorities)
	for i := range requests {
		requests[i] = newTimeBucketedCounter(now, d/10, 10)
		accepts[i] = newTimeBucketedCounter(now, d/10, 10)
	}

	return AdaptiveThrottle{
		k:        k,
		requests: requests,
		accepts:  accepts,
	}
}

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

type timeBucketedCounter struct {
	width time.Duration

	last    time.Time
	count   int
	buckets []int
	head    int
}

func newTimeBucketedCounter(now time.Time, width time.Duration, n int) timeBucketedCounter {
	return timeBucketedCounter{
		width:   width,
		last:    now,
		buckets: make([]int, n),
	}
}

func (c *timeBucketedCounter) add(now time.Time, x int) {
	c.get(now)
	c.buckets[c.head] += x
	c.count += x
}

func (c *timeBucketedCounter) get(now time.Time) int {
	elapsed := now.Sub(c.last)
	bucketsPassed := int(elapsed / c.width)
	if bucketsPassed >= len(c.buckets) {
		bucketsPassed = len(c.buckets)
	}
	for i := 0; i < bucketsPassed; i++ {
		nextIdx := (c.head + 1) % len(c.buckets)
		c.count -= c.buckets[nextIdx]
		c.buckets[nextIdx] = 0
		c.head = nextIdx
	}
	if bucketsPassed > 0 {
		c.last = now
	}
	return c.count
}
