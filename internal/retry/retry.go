// Package retry runs an operation with capped exponential backoff, full jitter and a retry budget.
package retry

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"
)

var ErrBudgetExhausted = errors.New("retry budget exhausted")

type Policy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration

	// Test hooks; nil means math/rand/v2 and a ctx-aware timer.
	jitter func(n int64) int64
	sleep  func(ctx context.Context, d time.Duration) error
}

func DefaultPolicy() Policy {
	return Policy{MaxAttempts: 3, BaseDelay: 50 * time.Millisecond, MaxDelay: time.Second}
}

func (p Policy) withDefaults() Policy {
	def := DefaultPolicy()
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = def.MaxAttempts
	}
	if p.BaseDelay <= 0 {
		p.BaseDelay = def.BaseDelay
	}
	if p.MaxDelay <= 0 {
		p.MaxDelay = def.MaxDelay
	}
	if p.jitter == nil {
		p.jitter = rand.Int64N
	}
	if p.sleep == nil {
		p.sleep = sleepCtx
	}
	return p
}

// backoff returns the full-jitter delay before attempt n (n >= 2).
func (p Policy) backoff(n int) time.Duration {
	ceiling := p.MaxDelay
	if shift := n - 2; shift < 62 {
		if d := p.BaseDelay << shift; d>>shift == p.BaseDelay && d < ceiling {
			ceiling = d
		}
	}
	return time.Duration(p.jitter(int64(ceiling) + 1))
}

// Do calls fn until it succeeds, classify says the error is final, attempts run out,
// the budget is empty, or the next sleep would cross ctx's deadline. It returns the
// number of attempts made and the last error (wrapped with ErrBudgetExhausted when
// the budget stopped it).
func Do(ctx context.Context, p Policy, b *Budget, classify func(error) (retryable bool, after time.Duration), fn func(ctx context.Context) error) (int, error) {
	p = p.withDefaults()
	b.Deposit()
	for attempt := 1; ; attempt++ {
		err := fn(ctx)
		if err == nil {
			return attempt, nil
		}
		if attempt >= p.MaxAttempts {
			return attempt, err
		}
		retryable, after := classify(err)
		if !retryable {
			return attempt, err
		}
		delay := max(p.backoff(attempt+1), after)
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < delay {
			return attempt, err
		}
		if !b.Withdraw() {
			return attempt, fmt.Errorf("%w: %w", ErrBudgetExhausted, err)
		}
		if p.sleep(ctx, delay) != nil {
			return attempt, err
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
