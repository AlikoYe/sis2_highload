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

// maxShift keeps BaseDelay<<shift inside int64; larger shifts are capped at MaxDelay anyway.
const maxShift = 62

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
	defaults := DefaultPolicy()
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = defaults.MaxAttempts
	}
	if p.BaseDelay <= 0 {
		p.BaseDelay = defaults.BaseDelay
	}
	if p.MaxDelay <= 0 {
		p.MaxDelay = defaults.MaxDelay
	}
	if p.jitter == nil {
		p.jitter = rand.Int64N
	}
	if p.sleep == nil {
		p.sleep = sleepCtx
	}
	return p
}

// backoff returns the full-jitter delay before attempt n (n >= 2):
// uniform in [0, min(MaxDelay, BaseDelay*2^(n-2))].
func (p Policy) backoff(attempt int) time.Duration {
	ceiling := p.MaxDelay
	if shift := attempt - 2; shift < maxShift {
		exponential := p.BaseDelay << shift
		overflowed := exponential>>shift != p.BaseDelay
		if !overflowed && exponential < ceiling {
			ceiling = exponential
		}
	}
	return time.Duration(p.jitter(int64(ceiling) + 1))
}

// Do calls fn until it succeeds, classify says the error is final, attempts run out,
// the budget is empty, or the next sleep would cross ctx's deadline. It returns the
// number of attempts made and the last error (wrapped with ErrBudgetExhausted when
// the budget stopped it).
func Do(
	ctx context.Context,
	policy Policy,
	budget *Budget,
	classify func(error) (retryable bool, after time.Duration),
	fn func(ctx context.Context) error,
) (int, error) {
	policy = policy.withDefaults()
	budget.Deposit()
	for attempt := 1; ; attempt++ {
		err := fn(ctx)
		if err == nil {
			return attempt, nil
		}
		if attempt >= policy.MaxAttempts {
			return attempt, err
		}
		retryable, after := classify(err)
		if !retryable {
			return attempt, err
		}
		delay := max(policy.backoff(attempt+1), after)
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < delay {
			return attempt, err
		}
		if !budget.Withdraw() {
			return attempt, fmt.Errorf("%w: %w", ErrBudgetExhausted, err)
		}
		if policy.sleep(ctx, delay) != nil {
			return attempt, err
		}
	}
}

func sleepCtx(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
