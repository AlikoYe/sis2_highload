package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"sis2_highload/internal/domain"
)

type Service struct {
	store Store
	now   func() time.Time
}

func New(store Store) *Service {
	return &Service{store: store, now: time.Now}
}

func (s *Service) Publish(ctx context.Context, ev domain.Event) (domain.Result, error) {
	if err := ctx.Err(); err != nil {
		return domain.Result{}, err
	}
	item, err := s.prepare(ev)
	if err != nil {
		return domain.Result{}, err
	}
	results, err := s.create(ctx, []NewEvent{item})
	if err != nil {
		return domain.Result{}, err
	}
	return results[0], nil
}

func (s *Service) PublishBatch(ctx context.Context, events []domain.Event) ([]domain.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(events) == 0 || len(events) > MaxBatchSize {
		return nil, fmt.Errorf("%w: batch size %d, want 1..%d", domain.ErrInvalidArgument, len(events), MaxBatchSize)
	}
	items := make([]NewEvent, len(events))
	for i, ev := range events {
		item, err := s.prepare(ev)
		if err != nil {
			return nil, fmt.Errorf("events[%d]: %w", i, err)
		}
		items[i] = item
	}
	return s.create(ctx, items)
}

func (s *Service) GetStatus(ctx context.Context, eventID string) (domain.Result, error) {
	if err := ctx.Err(); err != nil {
		return domain.Result{}, err
	}
	id, err := normalizeID(eventID)
	if err != nil {
		return domain.Result{}, err
	}
	ds, err := s.store.GetDeliveries(ctx, id)
	if err != nil {
		return domain.Result{}, storeErr(err)
	}
	return domain.NewResult(id, ds), nil
}

func (s *Service) create(ctx context.Context, items []NewEvent) ([]domain.Result, error) {
	results, err := s.store.CreateDeliveries(ctx, items)
	if err != nil {
		return nil, storeErr(err)
	}
	if len(results) != len(items) {
		return nil, fmt.Errorf("%w: store returned %d results for %d events", domain.ErrUnavailable, len(results), len(items))
	}
	return results, nil
}

// storeErr passes our sentinel errors through and maps everything else
// (connection loss, timeouts) to ErrUnavailable, keeping the cause wrapped.
func storeErr(err error) error {
	for _, known := range []error{domain.ErrInvalidArgument, domain.ErrNotFound, domain.ErrUnavailable, domain.ErrOverloaded} {
		if errors.Is(err, known) {
			return err
		}
	}
	return fmt.Errorf("%w: store: %w", domain.ErrUnavailable, err)
}
