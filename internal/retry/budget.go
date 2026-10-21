package retry

import "sync"

// Budget is a token bucket that limits retries to a fraction of first attempts,
// so a struggling server is not hit with a retry storm. A nil *Budget is unlimited.
type Budget struct {
	mu sync.Mutex
	// Kept in thousandths of a token so ratios like 0.1 add up exactly.
	milli, maxMilli, ratioMilli int64
}

func NewBudget(initial, max, ratio float64) *Budget {
	return &Budget{milli: int64(initial * 1000), maxMilli: int64(max * 1000), ratioMilli: int64(ratio * 1000)}
}

// DefaultBudget starts with 10 tokens, caps at 100 and earns 0.1 per first attempt.
func DefaultBudget() *Budget { return NewBudget(10, 100, 0.1) }

func (b *Budget) Deposit() {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.milli = min(b.maxMilli, b.milli+b.ratioMilli)
	b.mu.Unlock()
}

// Withdraw takes one token for a retry and reports whether one was available.
func (b *Budget) Withdraw() bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.milli < 1000 {
		return false
	}
	b.milli -= 1000
	return true
}

func (b *Budget) Tokens() float64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return float64(b.milli) / 1000
}
