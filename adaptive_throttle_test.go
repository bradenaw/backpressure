package backpressure

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"text/tabwriter"
	"time"

	"golang.org/x/time/rate"
)

func TestAdaptiveThrottleBasic(t *testing.T) {
	duration := 30 * time.Second
	start := time.Now()

	demandRates := []int{5, 10, 20}
	supplyRate := 20.0
	acceptedErrorPct := 0.1

	serverLimiter := NewRateLimiter(len(demandRates), supplyRate, supplyRate)

	clientThrottle := NewAdaptiveThrottle(len(demandRates), AdaptiveThrottleWindow(3*time.Second))

	var wg sync.WaitGroup
	requestsByPriority := make([]int, len(demandRates))
	sentByPriority := make([]int, len(demandRates))
	for i, r := range demandRates {
		i := i
		p := Priority(i)
		l := rate.NewLimiter(rate.Limit(r), r)

		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Since(start) < duration {
				l.Wait(context.Background())

				requestsByPriority[i]++
				_, _ = WithAdaptiveThrottle(clientThrottle, p, func() (struct{}, error) {
					sentByPriority[i]++
					err := serverLimiter.Wait(context.Background(), p, 1)
					if err != nil {
						return struct{}{}, err
					}
					if rand.Float64() < acceptedErrorPct {
						return struct{}{}, AcceptedError(errors.New("not really an error"))
					}
					return struct{}{}, nil
				})
			}
		}()
	}
	wg.Wait()
	realDuration := time.Since(start)

	totalSent := 0
	for _, sent := range sentByPriority {
		totalSent += sent
	}
	sendRate := float64(totalSent) / realDuration.Seconds()

	t.Logf("total supply:        %.2f", supplyRate)
	t.Logf("aggregate sent rate: %.2f", sendRate)
	var sb strings.Builder
	tw := tabwriter.NewWriter(&sb, 0, 4, 2, ' ', 0)
	fmt.Fprint(tw, "priority\trequest rate\tsend rate\treject %\n")
	for i := range demandRates {
		fmt.Fprintf(
			tw,
			"%d\t%.2f/sec\t%.2f/sec\t%.2f%%\n",
			i,
			float64(requestsByPriority[i])/realDuration.Seconds(),
			float64(sentByPriority[i])/realDuration.Seconds(),
			float64(requestsByPriority[i]-sentByPriority[i])/float64(requestsByPriority[i])*100,
		)
	}
	tw.Flush()
	t.Log("\n" + sb.String())
}
