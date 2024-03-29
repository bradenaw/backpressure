package backpressure

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"text/tabwriter"
	"time"
)

func TestRateLimiterStress(t *testing.T) {
	d := 10 * time.Second
	rate := float64(20)
	qpsByPriority := []int{5, 17, 10}

	rl := NewRateLimiter(
		len(qpsByPriority),
		rate,
		100,
		RateLimiterShortTimeout(5*time.Millisecond),
		RateLimiterLongTimeout(100*time.Millisecond),
		RateLimiterDebtDecayInterval(10*time.Second),
		RateLimiterDebtForgivePerSuccess(0.1),
	)
	defer rl.Close()

	accepts := make([]uint32, len(qpsByPriority))
	rejects := make([]uint32, len(qpsByPriority))
	waits := make([]int64, len(qpsByPriority))

	start := time.Now()
	var wg sync.WaitGroup
	for p := 0; p < len(qpsByPriority); p++ {
		wg.Add(1)
		p := p
		go func() {
			defer wg.Done()
			for time.Since(start) < d {
				time.Sleep(time.Second / time.Duration(qpsByPriority[p]))

				waitStart := time.Now()
				err := rl.Wait(context.Background(), Priority(p), 1)
				atomic.AddInt64(&waits[p], int64(time.Since(waitStart).Nanoseconds()))
				if err != nil {
					atomic.AddUint32(&rejects[p], 1)
				} else {
					atomic.AddUint32(&accepts[p], 1)
				}
			}
		}()
	}
	wg.Wait()

	var sb strings.Builder
	tw := tabwriter.NewWriter(&sb, 0, 4, 2, ' ', 0)
	fmt.Fprint(tw, "priority\taccepts\trejects\tdebt\tavg wait\n")
	now := time.Now()
	totalAccepts := 0
	for p := range accepts {
		fmt.Fprintf(
			tw,
			"%d\t%d\t%d\t%.2f\t%s\n",
			p,
			accepts[p],
			rejects[p],
			rl.debt[p].get(now),
			time.Duration(waits[p]/int64(accepts[p]))*time.Nanosecond,
		)
		totalAccepts += int(accepts[p])
	}
	tw.Flush()
	t.Log("\n" + sb.String())
	t.Logf("%.2f / sec (%.2f desired)", float64(totalAccepts)/d.Seconds(), rate)
}
