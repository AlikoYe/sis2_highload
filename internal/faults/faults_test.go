package faults

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNilInjectorIsNoop(t *testing.T) {
	in := New(Config{})
	if in != nil {
		t.Fatal("disabled config should give a nil injector")
	}
	if err := in.Before(context.Background()); err != nil || in.LoseResponse() {
		t.Fatal("nil injector must not inject")
	}
}

func TestAlwaysFail(t *testing.T) {
	in := New(Config{FailBefore: 1, FailAfter: 1})
	if err := in.Before(context.Background()); !errors.Is(err, ErrFailBefore) {
		t.Fatalf("Before = %v, want ErrFailBefore", err)
	}
	if !in.LoseResponse() {
		t.Fatal("LoseResponse = false with FailAfter=1")
	}
}

func TestLatencyRespectsContext(t *testing.T) {
	in := New(Config{Latency: time.Second})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := in.Before(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Before = %v, want DeadlineExceeded", err)
	}
	if took := time.Since(start); took > 200*time.Millisecond {
		t.Fatalf("took %v, ignored ctx", took)
	}
}

func TestValidate(t *testing.T) {
	for _, c := range []Config{{FailBefore: -0.1}, {FailAfter: 1.5}, {Latency: -time.Second}} {
		if c.Validate() == nil {
			t.Fatalf("Validate(%+v) = nil, want error", c)
		}
	}
	if err := (Config{FailBefore: 0.3, Latency: time.Millisecond}).Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
}
