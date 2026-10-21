package memory

import (
	"context"
	"fmt"
	"sync"

	"sis2_highload/internal/domain"
	"sis2_highload/internal/service"
)

type Store struct {
	mu   sync.Mutex
	data map[string]map[domain.Channel]domain.Delivery

	// Optional FIFO cap so long benchmarks do not grow the heap without bound.
	maxEvents int
	order     []string
	next      int
}

func New() *Store { return NewBounded(0) }

// NewBounded keeps at most maxEvents events, evicting the oldest first; 0 means unbounded.
// Idempotency then only holds for the most recent maxEvents events.
func NewBounded(maxEvents int) *Store {
	return &Store{data: make(map[string]map[domain.Channel]domain.Delivery), maxEvents: maxEvents}
}

func (s *Store) track(id string) {
	if s.maxEvents <= 0 {
		return
	}
	if len(s.order) < s.maxEvents {
		s.order = append(s.order, id)
		return
	}
	delete(s.data, s.order[s.next])
	s.order[s.next] = id
	s.next = (s.next + 1) % s.maxEvents
}

func (s *Store) CreateDeliveries(ctx context.Context, items []service.NewEvent) ([]domain.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	results := make([]domain.Result, len(items))
	for i, it := range items {
		id := it.Event.ID
		byChannel := s.data[id]
		if byChannel == nil {
			byChannel = make(map[domain.Channel]domain.Delivery, len(it.Channels))
			s.data[id] = byChannel
			s.track(id)
		}
		ds := make([]domain.Delivery, 0, len(it.Channels))
		for _, ch := range it.Channels {
			d, ok := byChannel[ch]
			if !ok {
				d = domain.Delivery{EventID: id, Channel: ch, Status: domain.StatusQueued}
				byChannel[ch] = d
			}
			ds = append(ds, d)
		}
		results[i] = domain.NewResult(id, ds)
	}
	return results, nil
}

func (s *Store) GetDeliveries(ctx context.Context, eventID string) ([]domain.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	byChannel := s.data[eventID]
	if len(byChannel) == 0 {
		return nil, fmt.Errorf("%w: event %s", domain.ErrNotFound, eventID)
	}
	ds := make([]domain.Delivery, 0, len(byChannel))
	for _, d := range byChannel {
		ds = append(ds, d)
	}
	domain.SortDeliveries(ds)
	return ds, nil
}

func (s *Store) Ping(context.Context) error { return nil }
