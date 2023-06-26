package backpressure

import (
	"math"
	"time"
)

type linearDecay struct {
	x           float64
	last        time.Time
	decayPerSec float64
	max         float64
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

func (d *linearDecay) get(now time.Time) float64 {
	d.x = math.Max(0, d.x-math.Max(0, now.Sub(d.last).Seconds())*d.decayPerSec)
	d.last = now
	return d.x
}
