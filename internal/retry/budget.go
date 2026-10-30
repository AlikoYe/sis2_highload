package retry

import "sync"

const milliPerToken = 1000

// Budget is a token bucket that limits retries to a fraction of first attempts,
// so a struggling server is not hit with a retry storm. A nil *Budget is unlimited.
type Budget struct {
	mu sync.Mutex
	// Kept in thousandths of a token so ratios like 0.1 add up exactly.
	milli, capacityMilli, ratioMilli int64
}

// NewBudget starts with initial tokens, holds at most capacity and earns ratio per first attempt.
func NewBudget(initial, capacity, ratio float64) *Budget {
	return &Budget{
		milli:         int64(initial * milliPerToken),
		capacityMilli: int64(capacity * milliPerToken),
		ratioMilli:    int64(ratio * milliPerToken),
	}
}

// DefaultBudget starts with 10 tokens, caps at 100 and earns 0.1 per first attempt.
func DefaultBudget() *Budget { return NewBudget(10, 100, 0.1) }

func (b *Budget) Deposit() {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.milli = min(b.capacityMilli, b.milli+b.ratioMilli)
	b.mu.Unlock()
}

// Withdraw takes one token for a retry and reports whether one was available.
func (b *Budget) Withdraw() bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.milli < milliPerToken {
		return false
	}
	b.milli -= milliPerToken
	return true
}

func (b *Budget) Tokens() float64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return float64(b.milli) / milliPerToken
}
