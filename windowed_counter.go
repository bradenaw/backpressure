package backpressure

import (
	"time"
)

type windowedCounter struct {
	width time.Duration

	last    time.Time
	count   int
	buckets []int
	head    int
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
	bucketsPassed := int(elapsed / c.width)
	if bucketsPassed < 0 {
		bucketsPassed = 0
	}
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
