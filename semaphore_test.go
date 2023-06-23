package backpressure

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"testing"
	"text/tabwriter"
	"time"

	"golang.org/x/sync/errgroup"
)

func TestSemaphoreStress(t *testing.T) {
	concurrent := int32(0)

	capacity := 20
	nUsers := []int{5, 10, 10, 1}
	accepts := make([]uint32, len(nUsers))
	rejects := make([]uint32, len(nUsers))
	waits := make([]int64, len(nUsers))

	duration := 10 * time.Second
	holdTime := 9 * time.Millisecond

	sem := NewSemaphore(
		len(nUsers),
		capacity,
		SemaphoreShortTimeout(5*time.Millisecond),
		SemaphoreLongTimeout(100*time.Millisecond),
		SemaphoreDebtDecayInterval(10*time.Second),
		SemaphoreDebtForgivePerSuccess(0.10),
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
					err := sem.Acquire(context.Background(), Priority(p), 1)
					atomic.AddInt64(&waits[p], time.Since(admitStart).Nanoseconds())
					if err != nil {
						atomic.AddUint32(&rejects[p], 1)
						time.Sleep(jitter(holdTime))
						continue
					}
					atomic.AddUint32(&accepts[p], 1)

					if int(atomic.AddInt32(&concurrent, 1)) > capacity {
						return errors.New("too many concurrent holders")
					}
					time.Sleep(holdTime)
					atomic.AddInt32(&concurrent, -1)
					sem.Release(1)
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

	var sb strings.Builder
	tw := tabwriter.NewWriter(&sb, 0, 4, 2, ' ', 0)
	fmt.Fprint(tw, "priority\tacquires\trejects\tdebt\tavg wait\n")
	now := time.Now()
	for p := range accepts {
		avgWaitStr := "inf"
		if accepts[p] > 0 {
			avgWaitStr = fmt.Sprint(time.Duration(waits[p]/int64(accepts[p])) * time.Nanosecond)
		}
		fmt.Fprintf(
			tw,
			"%d\t%d\t%d\t%.2f\t%s\n",
			p,
			accepts[p],
			rejects[p],
			sem.debt[p].get(now),
			avgWaitStr,
		)
	}
	tw.Flush()
	t.Log("\n" + sb.String())
}

func jitter(d time.Duration) time.Duration {
	return d/2 + time.Duration(rand.Int63n(int64(d)))
}
