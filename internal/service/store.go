package service

import (
	"context"

	"sis2_highload/internal/domain"
)

type NewEvent struct {
	Event    domain.Event
	Channels []domain.Channel
}

type Store interface {
	// CreateDeliveries idempotently creates one QUEUED delivery per (event, channel)
	// and returns results in input order; existing rows are returned as they are.
	CreateDeliveries(ctx context.Context, items []NewEvent) ([]domain.Result, error)
	// GetDeliveries returns domain.ErrNotFound if the event has no deliveries.
	GetDeliveries(ctx context.Context, eventID string) ([]domain.Delivery, error)
}
