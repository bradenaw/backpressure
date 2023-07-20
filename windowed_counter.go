package backpressure

import (
	"time"
)

// windowedCounter counts events in an approximate time window. It does this by splitting time into
// buckets of some width and removing buckets that are too old.
type windowedCounter struct {
	// The width of a single bucket.
	width time.Duration

	// The last time the bucket was read or written.
	last time.Time
	// The sum of all buckets.
	count int
	// The count of evens that happened in each bucket. This is a circular buffer.
	buckets []int
	// The index of the 'head' of the circular buffer, that is, the bucket that corresponds to
	// `last`.
	head int
}

func newWindowedCounter(now time.Time, width time.Duration, n int) windowedCounter {
	return windowedCounter{
		width:   width,
		last:    now,
		buckets: make([]int, n),
	}
}

func (c *windowedCounter) add(now time.Time, x int) {
	c.get(now)
	c.buckets[c.head] += x
	c.count += x
}

func (c *windowedCounter) get(now time.Time) int {
	elapsed := now.Sub(c.last)

	// How many buckets have we passed since `last`?
	bucketsPassed := int(elapsed / c.width)
	if bucketsPassed < 0 {
		bucketsPassed = 0
	}
	// Since it's a circular buffer, passing more than all of the buckets is the same as passing all
	// of them.
	if bucketsPassed >= len(c.buckets) {
		bucketsPassed = len(c.buckets)
	}

	// For all of the buckets that already happened, zero them out, advance head, and remove their
	// amounts from c.count.
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
