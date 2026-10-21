package main

import (
	"context"
	"errors"
	"sync/atomic"

	"sis2_highload/internal/client"
	"sis2_highload/internal/domain"
)

// roundRobin spreads calls across clients that each own one connection.
type roundRobin struct {
	cs   []client.Client
	next atomic.Uint64
}

func (r *roundRobin) pick() client.Client { return r.cs[(r.next.Add(1)-1)%uint64(len(r.cs))] }

func (r *roundRobin) Publish(ctx context.Context, e domain.Event) (domain.Result, error) {
	return r.pick().Publish(ctx, e)
}

func (r *roundRobin) PublishBatch(ctx context.Context, es []domain.Event) ([]domain.Result, error) {
	return r.pick().PublishBatch(ctx, es)
}

func (r *roundRobin) GetStatus(ctx context.Context, id string) (domain.Result, error) {
	return r.pick().GetStatus(ctx, id)
}

func (r *roundRobin) Stats() client.Stats {
	var total client.Stats
	for _, c := range r.cs {
		s := c.Stats()
		total.Calls += s.Calls
		total.Attempts += s.Attempts
		total.Retries += s.Retries
		total.BudgetExhausted += s.BudgetExhausted
	}
	return total
}

func (r *roundRobin) Close() error {
	var errs []error
	for _, c := range r.cs {
		errs = append(errs, c.Close())
	}
	return errors.Join(errs...)
}
