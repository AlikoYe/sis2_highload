// Package client calls the notification service over gRPC or REST with
// per-call deadlines and one shared retry policy.
package client

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"sis2_highload/internal/domain"
	"sis2_highload/internal/retry"
)

type Client interface {
	Publish(ctx context.Context, e domain.Event) (domain.Result, error)
	PublishBatch(ctx context.Context, es []domain.Event) ([]domain.Result, error)
	GetStatus(ctx context.Context, eventID string) (domain.Result, error)
	Stats() Stats
	Close() error
}

// DefaultCallTimeout bounds one call including all of its retries.
const DefaultCallTimeout = 300 * time.Millisecond

type Options struct {
	CallTimeout time.Duration // default DefaultCallTimeout; covers all attempts of one call
	Policy      retry.Policy
	Budget      *retry.Budget // nil = unlimited retries (still capped by Policy)
}

type Stats struct {
	Calls, Attempts, Retries, BudgetExhausted int64
}

// NewEventID returns a UUIDv7: time-ordered, so it indexes well in Postgres.
func NewEventID() string {
	return uuid.Must(uuid.NewV7()).String()
}

// RemoteError is a failure reported by the server, mapped back to a domain error via Kind.
type RemoteError struct {
	Kind       error
	Message    string
	RetryAfter time.Duration
}

func (e *RemoteError) Error() string {
	if e.Message == "" {
		return "remote: " + e.Kind.Error()
	}
	return "remote: " + e.Message
}

func (e *RemoteError) Unwrap() error { return e.Kind }

// caller holds what both protocol clients share: options, retry loop and counters.
type caller struct {
	opts                                Options
	calls, attempts, retries, exhausted atomic.Int64
}

func newCaller(opts Options) caller {
	if opts.CallTimeout <= 0 {
		opts.CallTimeout = DefaultCallTimeout
	}
	return caller{opts: opts}
}

// call runs fn under the call deadline with retries; fn must resend the same request each time.
func (c *caller) call(ctx context.Context, fn func(ctx context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, c.opts.CallTimeout)
	defer cancel()
	c.calls.Add(1)
	attempts, err := retry.Do(ctx, c.opts.Policy, c.opts.Budget, classify, fn)
	c.attempts.Add(int64(attempts))
	c.retries.Add(int64(attempts - 1))
	if errors.Is(err, retry.ErrBudgetExhausted) {
		c.exhausted.Add(1)
	}
	return err
}

func (c *caller) Stats() Stats {
	return Stats{
		Calls:           c.calls.Load(),
		Attempts:        c.attempts.Load(),
		Retries:         c.retries.Load(),
		BudgetExhausted: c.exhausted.Load(),
	}
}

// classify retries only temporary server-side conditions. Deadline errors are final:
// the server's deadline is derived from ours, so if it expired ours has (nearly) too.
func classify(err error) (bool, time.Duration) {
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return false, 0
	case errors.Is(err, domain.ErrUnavailable), errors.Is(err, domain.ErrOverloaded):
		var remoteErr *RemoteError
		if errors.As(err, &remoteErr) {
			return true, remoteErr.RetryAfter
		}
		return true, 0
	}
	return false, 0
}
