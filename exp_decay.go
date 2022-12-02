package backpressure

import (
	"math"
	"time"
)

// expDecay is a number that decays exponentially over time.
//
// decay is a value in [0, 1), the percentage that each added value loses per second.
//
// e.g. for a decay of 3%, decay is 0.03
//
// time              15 seconds ago       5 seconds ago    0.5 second ago
// event             add(30)              add(17)          add(51)
// coeff             0.63                 0.85             .98
// x * coeff         19.0                 14.6             50.2
// ctr                                                                 = 83.8
type expDecay struct {
	decay float64
	last  time.Time
	ctr   float64
}

func (d *expDecay) add(now time.Time, x float64) {
	d.get(now)
	d.ctr += x
}

func (d *expDecay) get(now time.Time) float64 {
	d.ctr *= math.Pow((1 - d.decay), now.Sub(d.last).Seconds())
	d.last = now
	return d.ctr
}
