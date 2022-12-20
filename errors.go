package backpressure

import (
	"errors"
)

var (
	// ErrRejected is returned by RateLimiter and Semaphore when a request times out before being
	// admitted.
	ErrRejected = errors.New("rejected")

	// ErrClientRejection is returned by an AdaptiveThrottle when the request was not even sent to
	// the backend because it is overloaded.
	ErrClientRejection = errors.New("rejected without sending: backend is unhealthy")

	errAlreadyClosed = errors.New("already closed")

	errZeroRate = errors.New("RateLimiter with 0 rate rejects everything")

	errZeroCapacity = errors.New("Semaphore with 0 capacity rejects everything")
)
