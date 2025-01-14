package backpressure

import (
	"math"
	"time"
)

// linearDecay is a value in [0, max] that decays linearly towards zero over time.
type linearDecay struct {
	// x is the value of linearDecay as of last
	x float64
	// last is the last time x was computed
	last time.Time
	// decayPerSec is subtracted from x every second
	decayPerSec float64
	// max is the maximum value of x
	max float64
}

func (d *linearDecay) add(now time.Time, x float64) {
	d.get(now)
	d.x += x
	if d.x > d.max {
		d.x = d.max
	}
	if d.x < 0 {
		d.x = 0
	}
}

func (d *linearDecay) setDecayPerSec(now time.Time, decayPerSec float64) {
	d.get(now)
	d.decayPerSec = decayPerSec
}

func (d *linearDecay) setMax(now time.Time, max float64) {
	d.get(now)
	d.max = max
	if d.x > d.max {
		d.x = d.max
	}
}

func (d *linearDecay) floor(now time.Time, floor float64) {
	d.get(now)
	if d.x < floor {
		d.x = floor
	}
}

func (d *linearDecay) get(now time.Time) float64 {
	d.x = math.Max(0, d.x-math.Max(0, now.Sub(d.last).Seconds())*d.decayPerSec)
	d.last = now
	return d.x
}
