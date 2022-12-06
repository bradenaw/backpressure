package backpressure

import (
	"context"
	"errors"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
)

func TestSemaphoreStress(t *testing.T) {
	concurrent := int32(0)

	capacity := 20
	nUsers := []int{5, 10, 10, 1}
	acquires := make([]uint32, len(nUsers))
	rejections := make([]uint32, len(nUsers))
	waits := make([]int64, len(nUsers))

	duration := 10 * time.Second
	holdTime := 9 * time.Millisecond

	sem := NewSemaphore(
		len(nUsers),
		5*time.Millisecond,
		100*time.Millisecond,
		capacity,
		0.05,
		0.10,
	)
	defer sem.Close()

	start := time.Now()
	var eg errgroup.Group

	for p, pUsers := range nUsers {
		p := p
		for i := 0; i < pUsers; i++ {
			eg.Go(func() error {
				for time.Since(start) < duration {
					admitStart := time.Now()
					ticket, err := sem.Acquire(context.Background(), Priority(p))
					if err != nil {
						atomic.AddUint32(&rejections[p], 1)
						time.Sleep(jitter(holdTime))
						continue
					}
					atomic.AddInt64(&waits[p], time.Since(admitStart).Nanoseconds())
					atomic.AddUint32(&acquires[p], 1)

					if int(atomic.AddInt32(&concurrent, 1)) > capacity {
						return errors.New("too many concurrent holders")
					}
					time.Sleep(holdTime)
					atomic.AddInt32(&concurrent, -1)
					ticket.Release()
					time.Sleep(jitter(holdTime / 4))
				}
				return nil
			})
		}
	}

	err := eg.Wait()
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("acquires: %v", acquires)
	t.Logf("rejections: %v", rejections)
	for i := range sem.debt {
		t.Logf("debt[%d]: %f", i, sem.debt[i].get(time.Now()))
	}
	for i := range waits {
		t.Logf("avgWait: %s", time.Duration(waits[i]/int64(acquires[i])))
	}
}

func jitter(d time.Duration) time.Duration {
	return d/2 + time.Duration(rand.Int63n(int64(d)))
}
