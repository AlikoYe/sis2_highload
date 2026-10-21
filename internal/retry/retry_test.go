package retry

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"
)

var errTemp = errors.New("temporary")

func retryable(error) (bool, time.Duration) { return true, 0 }

// fixture records sleeps instead of sleeping; jitter returns the top of the range.
type fixture struct {
	sleeps []time.Duration
}

func (f *fixture) policy(maxAttempts int) Policy {
	return Policy{
		MaxAttempts: maxAttempts,
		BaseDelay:   50 * time.Millisecond,
		MaxDelay:    time.Second,
		jitter:      func(n int64) int64 { return n - 1 },
		sleep: func(_ context.Context, d time.Duration) error {
			f.sleeps = append(f.sleeps, d)
			return nil
		},
	}
}

func failing(n int) func(context.Context) error {
	calls := 0
	return func(context.Context) error {
		calls++
		if calls <= n {
			return errTemp
		}
		return nil
	}
}

func TestBackoffCeilingsAndCap(t *testing.T) {
	var f fixture
	attempts, err := Do(context.Background(), f.policy(8), nil, retryable, failing(100))
	if attempts != 8 || !errors.Is(err, errTemp) {
		t.Fatalf("attempts=%d err=%v, want 8 and errTemp", attempts, err)
	}
	ms := time.Millisecond
	want := []time.Duration{50 * ms, 100 * ms, 200 * ms, 400 * ms, 800 * ms, time.Second, time.Second}
	if !slices.Equal(f.sleeps, want) {
		t.Fatalf("sleeps = %v, want %v", f.sleeps, want)
	}
}

func TestBackoffJitterBounds(t *testing.T) {
	p := Policy{BaseDelay: 50 * time.Millisecond, MaxDelay: time.Second}.withDefaults()
	for n := 2; n <= 10; n++ {
		ceiling := min(time.Second, 50*time.Millisecond<<(n-2))
		for range 1000 {
			if d := p.backoff(n); d < 0 || d > ceiling {
				t.Fatalf("attempt %d: delay %v outside [0, %v]", n, d, ceiling)
			}
		}
	}
	if d := p.backoff(200); d < 0 || d > time.Second {
		t.Fatalf("huge attempt number: delay %v, want capped at 1s", d)
	}
}

func TestSucceedsAfterRetries(t *testing.T) {
	var f fixture
	attempts, err := Do(context.Background(), f.policy(5), nil, retryable, failing(2))
	if attempts != 3 || err != nil {
		t.Fatalf("attempts=%d err=%v, want 3 and nil", attempts, err)
	}
}

func TestRetryAfterIsAFloor(t *testing.T) {
	var f fixture
	hint := func(error) (bool, time.Duration) { return true, 300 * time.Millisecond }
	if _, err := Do(context.Background(), f.policy(2), nil, hint, failing(1)); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if len(f.sleeps) != 1 || f.sleeps[0] != 300*time.Millisecond {
		t.Fatalf("sleeps = %v, want [300ms]", f.sleeps)
	}
}

func TestStopsBeforeDeadline(t *testing.T) {
	var f fixture
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	attempts, err := Do(ctx, f.policy(5), nil, retryable, failing(100))
	if attempts != 1 || !errors.Is(err, errTemp) || len(f.sleeps) != 0 {
		t.Fatalf("attempts=%d err=%v sleeps=%v, want 1 attempt, errTemp, no sleep", attempts, err, f.sleeps)
	}
}

func TestRealSleepRespectsDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	start := time.Now()
	p := Policy{MaxAttempts: 100, BaseDelay: 10 * time.Millisecond, MaxDelay: 20 * time.Millisecond}
	_, err := Do(ctx, p, nil, retryable, failing(1000))
	if !errors.Is(err, errTemp) {
		t.Fatalf("err = %v, want last attempt error", err)
	}
	if took := time.Since(start); took > 100*time.Millisecond {
		t.Fatalf("took %v, slept past the 80ms deadline", took)
	}
}

func TestNonRetryableStopsImmediately(t *testing.T) {
	var f fixture
	final := func(error) (bool, time.Duration) { return false, 0 }
	attempts, err := Do(context.Background(), f.policy(5), nil, final, failing(100))
	if attempts != 1 || !errors.Is(err, errTemp) || len(f.sleeps) != 0 {
		t.Fatalf("attempts=%d err=%v, want 1 and errTemp", attempts, err)
	}
}

func TestBudgetExhaustion(t *testing.T) {
	var f fixture
	b := NewBudget(1, 100, 0)
	attempts, err := Do(context.Background(), f.policy(5), b, retryable, failing(100))
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2 (one retry paid by the single token)", attempts)
	}
	if !errors.Is(err, ErrBudgetExhausted) || !errors.Is(err, errTemp) {
		t.Fatalf("err = %v, want ErrBudgetExhausted wrapping errTemp", err)
	}
}

func TestBudgetAccounting(t *testing.T) {
	b := DefaultBudget()
	for range 100 {
		b.Deposit()
	}
	if got := b.Tokens(); got != 20 {
		t.Fatalf("tokens = %v, want 10 + 100*0.1 = 20", got)
	}
	for range 2000 {
		b.Deposit()
	}
	if got := b.Tokens(); got != 100 {
		t.Fatalf("tokens = %v, want cap 100", got)
	}
	var nilBudget *Budget
	if !nilBudget.Withdraw() {
		t.Fatal("nil budget must be unlimited")
	}
}

func TestBudgetConcurrent(t *testing.T) {
	b := NewBudget(50, 1000, 0.5)
	var wg sync.WaitGroup
	var mu sync.Mutex
	withdrawn := 0
	for range 100 {
		wg.Go(func() {
			b.Deposit()
			if b.Withdraw() {
				mu.Lock()
				withdrawn++
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	// 50 + 100*0.5 = 100 tokens in, never below zero, so every withdraw succeeds.
	if withdrawn != 100 || b.Tokens() != 0 {
		t.Fatalf("withdrawn=%d tokens=%v, want 100 and 0", withdrawn, b.Tokens())
	}
}
