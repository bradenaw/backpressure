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
)