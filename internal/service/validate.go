package service

import (
	"fmt"
	"slices"

	"github.com/google/uuid"

	"sis2_highload/internal/domain"
)

const (
	MaxBatchSize    = 100
	MaxPayloadKeys  = 32
	MaxPayloadBytes = 4 << 10
)

// validate checks ev and returns it normalised (canonical id, UTC time) with its channels.
func (s *Service) validate(ev domain.Event) (NewEvent, error) {
	id, err := normalizeID(ev.ID)
	if err != nil {
		return NewEvent{}, err
	}
	channels, ok := channelsByType[ev.Type]
	if !ok {
		return NewEvent{}, fmt.Errorf("%w: unknown event type %q", domain.ErrInvalidArgument, ev.Type)
	}
	if ev.UserID <= 0 {
		return NewEvent{}, fmt.Errorf("%w: user_id must be > 0", domain.ErrInvalidArgument)
	}
	if len(ev.Payload) > MaxPayloadKeys {
		return NewEvent{}, fmt.Errorf("%w: payload has %d keys, max %d", domain.ErrInvalidArgument, len(ev.Payload), MaxPayloadKeys)
	}
	if size := payloadSize(ev.Payload); size > MaxPayloadBytes {
		return NewEvent{}, fmt.Errorf("%w: payload is %d bytes, max %d", domain.ErrInvalidArgument, size, MaxPayloadBytes)
	}

	ev.ID = id
	if ev.OccurredAt.IsZero() {
		ev.OccurredAt = s.now()
	}
	ev.OccurredAt = ev.OccurredAt.UTC()
	return NewEvent{Event: ev, Channels: slices.Clone(channels)}, nil
}

func payloadSize(payload map[string]string) int {
	size := 0
	for key, value := range payload {
		size += len(key) + len(value)
	}
	return size
}

// normalizeID returns the canonical lowercase form so every store keys events identically.
func normalizeID(id string) (string, error) {
	u, err := uuid.Parse(id)
	if err != nil {
		return "", fmt.Errorf("%w: event_id %q is not a valid UUID", domain.ErrInvalidArgument, id)
	}
	return u.String(), nil
}
