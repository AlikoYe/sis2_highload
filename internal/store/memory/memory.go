package memory

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"

	"sis2_highload/internal/domain"
	"sis2_highload/internal/service"
)

type Store struct {
	mu      sync.Mutex
	byEvent map[string]map[domain.Channel]domain.Delivery

	// Optional FIFO cap so long benchmarks do not grow the heap without bound.
	maxEvents      int
	insertionOrder []string // ring buffer of event ids once full
	oldest         int      // index of the next id to evict
}

func New() *Store { return NewBounded(0) }

// NewBounded keeps at most maxEvents events, evicting the oldest first; 0 means unbounded.
// Idempotency then only holds for the most recent maxEvents events.
func NewBounded(maxEvents int) *Store {
	return &Store{byEvent: make(map[string]map[domain.Channel]domain.Delivery), maxEvents: maxEvents}
}

func (s *Store) trackForEviction(id string) {
	if s.maxEvents <= 0 {
		return
	}
	if len(s.insertionOrder) < s.maxEvents {
		s.insertionOrder = append(s.insertionOrder, id)
		return
	}
	delete(s.byEvent, s.insertionOrder[s.oldest])
	s.insertionOrder[s.oldest] = id
	s.oldest = (s.oldest + 1) % s.maxEvents
}

func (s *Store) CreateDeliveries(ctx context.Context, items []service.NewEvent) ([]domain.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	results := make([]domain.Result, len(items))
	for i, item := range items {
		id := item.Event.ID
		byChannel := s.byEvent[id]
		if byChannel == nil {
			byChannel = make(map[domain.Channel]domain.Delivery, len(item.Channels))
			s.byEvent[id] = byChannel
			s.trackForEviction(id)
		}
		deliveries := make([]domain.Delivery, 0, len(item.Channels))
		for _, ch := range item.Channels {
			d, ok := byChannel[ch]
			if !ok {
				d = domain.Delivery{EventID: id, Channel: ch, Status: domain.StatusQueued}
				byChannel[ch] = d
			}
			deliveries = append(deliveries, d)
		}
		results[i] = domain.NewResult(id, deliveries)
	}
	return results, nil
}

func (s *Store) GetDeliveries(ctx context.Context, eventID string) ([]domain.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	byChannel := s.byEvent[eventID]
	if len(byChannel) == 0 {
		return nil, fmt.Errorf("%w: event %s", domain.ErrNotFound, eventID)
	}
	deliveries := slices.Collect(maps.Values(byChannel))
	domain.SortDeliveries(deliveries)
	return deliveries, nil
}

func (s *Store) Ping(context.Context) error { return nil }
