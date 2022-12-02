package backpressure

import (
	"context"
	"fmt"
)

type Priority int

const (
	Critical Priority = iota
	High
	Medium
	Low
)

func (p Priority) String() string {
	switch p {
	case Critical:
		return "CRITICAL"
	case High:
		return "HIGH"
	case Medium:
		return "MEDIUM"
	case Low:
		return "LOW"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", p)
	}
}

const nPriorities = 4

type contextKey struct{}

func ContextWithPriority(ctx context.Context, p Priority) context.Context {
	return context.WithValue(ctx, contextKey{}, p)
}

func ContextPriority(ctx context.Context) (Priority, bool) {
	v := ctx.Value(contextKey{})
	if v == nil {
		return Low, false
	}
	p := v.(Priority)
	if p < Critical || p > Low {
		return Low, false
	}
	return p, true
}
