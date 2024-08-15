package backpressure

import (
	"testing"
	"time"
)

func TestWindowedCounter(t *testing.T) {
	start := time.Unix(1723685600, 0)
	wc := newWindowedCounter(start, time.Second, 10)

	expect := func(now time.Time, v int) {
		actual := wc.get(now)
		if actual != v {
			t.Fatalf("at %s expected %d, got %d", now.Sub(start), v, actual)
		}
	}

	wc.add(start, 1)
	// c=  1
	// t=  0   1   2   3   4   5   6   7   8   9   10  11  12  13  14  15  16  17  18  19  20
	// w= [                                       ]
	expect(start, 1)

	wc.add(start.Add(1100*time.Millisecond), 1)
	// c=  1   1
	// t=  0   1   2   3   4   5   6   7   8   9   10  11  12  13  14  15  16  17  18  19  20
	// w= [                                       ]
	expect(start.Add(1400*time.Millisecond), 2)

	wc.add(start.Add(1500*time.Millisecond), 1)
	// c=  1   2
	// t=  0   1   2   3   4   5   6   7   8   9   10  11  12  13  14  15  16  17  18  19  20
	// w= [                                       ]
	expect(start.Add(1700*time.Millisecond), 3)

	wc.add(start.Add(2500*time.Millisecond), 4)
	// c=  1   2   4
	// t=  0   1   2   3   4   5   6   7   8   9   10  11  12  13  14  15  16  17  18  19  20
	// w= [                                       ]
	expect(start.Add(3100*time.Millisecond), 7)

	wc.add(start.Add(5500*time.Millisecond), 8)
	// c=  1   2   4           8
	// t=  0   1   2   3   4   5   6   7   8   9   10  11  12  13  14  15  16  17  18  19  20
	// w= [                                       ]
	expect(start.Add(7400*time.Millisecond), 15)

	wc.add(start.Add(9500*time.Millisecond), 16)
	// c=  1   2   4           8               16
	// t=  0   1   2   3   4   5   6   7   8   9   10  11  12  13  14  15  16  17  18  19  20
	// w= [                                       ]
	expect(start.Add(9800*time.Millisecond), 31)

	// c=  1   2   4           8               16
	// t=  0   1   2   3   4   5   6   7   8   9   10  11  12  13  14  15  16  17  18  19  20
	// w=         [                                       ]
	expect(start.Add(11500*time.Millisecond), 28)
	// c=  1   2   4           8               16
	// t=  0   1   2   3   4   5   6   7   8   9   10  11  12  13  14  15  16  17  18  19  20
	// w=                     [                                       ]
	expect(start.Add(14000*time.Millisecond), 24)
	// c=  1   2   4           8               16
	// t=  0   1   2   3   4   5   6   7   8   9   10  11  12  13  14  15  16  17  18  19  20
	// w=                                 [                                       ]
	expect(start.Add(17000*time.Millisecond), 16)

	wc.add(start.Add(31000*time.Millisecond), 32)
	expect(start.Add(37000*time.Millisecond), 32)
	expect(start.Add(45000*time.Millisecond), 0)
}

func TestWindowedCounterFrequent(t *testing.T) {
	start := time.Unix(1723685600, 0)
	wc := newWindowedCounter(start, time.Second, 10)

	expect := func(now time.Time, v int) {
		actual := wc.get(now)
		if actual != v {
			t.Fatalf("at %s expected %d, got %d", now.Sub(start), v, actual)
		}
	}

	for i := 0; i < 200; i++ {
		// 10 times/second for 20 seconds
		now := time.Duration(i) * 100 * time.Millisecond
		wc.add(start.Add(now), 1)
	}
	// if we advanced appropriately, we should have 100 and not 200
	expect(start.Add(19999*time.Millisecond), 100)
	expect(start.Add(40000*time.Millisecond), 0)
}
