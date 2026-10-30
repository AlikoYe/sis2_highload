// Package faults injects latency and failures into server requests for resilience tests.
package faults

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"sis2_highload/internal/domain"
)

var (
	ErrFailBefore   = fmt.Errorf("%w: injected fault before handling", domain.ErrUnavailable)
	ErrLostResponse = fmt.Errorf("%w: injected fault, response discarded after commit", domain.ErrUnavailable)
)

type Config struct {
	FailBefore    float64
	FailAfter     float64
	Latency       time.Duration
	LatencyJitter time.Duration
}

func (c Config) Enabled() bool {
	return c.FailBefore > 0 || c.FailAfter > 0 || c.Latency > 0 || c.LatencyJitter > 0
}

func (c Config) Validate() error {
	if c.FailBefore < 0 || c.FailBefore > 1 || c.FailAfter < 0 || c.FailAfter > 1 {
		return fmt.Errorf("fault probabilities must be in [0,1], got before=%v after=%v", c.FailBefore, c.FailAfter)
	}
	if c.Latency < 0 || c.LatencyJitter < 0 {
		return errors.New("fault latency must be >= 0")
	}
	return nil
}

// Injector is shared by both transports so faults behave identically. A nil *Injector injects nothing.
type Injector struct {
	cfg Config
}

// New returns nil when cfg injects nothing.
func New(cfg Config) *Injector {
	if !cfg.Enabled() {
		return nil
	}
	return &Injector{cfg: cfg}
}

// Before sleeps for the configured latency (respecting ctx) and may then fail
// the request before it reaches the service.
func (in *Injector) Before(ctx context.Context) error {
	if in == nil {
		return nil
	}
	if delay := in.latency(); delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if happens(in.cfg.FailBefore) {
		return ErrFailBefore
	}
	return nil
}

// LoseResponse reports whether a successful, already committed response should be discarded.
func (in *Injector) LoseResponse() bool {
	return in != nil && happens(in.cfg.FailAfter)
}

func (in *Injector) latency() time.Duration {
	delay := in.cfg.Latency
	if in.cfg.LatencyJitter > 0 {
		delay += rand.N(in.cfg.LatencyJitter + 1)
	}
	return delay
}

// happens returns true with the given probability.
func happens(probability float64) bool { return probability > 0 && rand.Float64() < probability }
