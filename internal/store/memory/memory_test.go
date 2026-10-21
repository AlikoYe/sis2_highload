package memory

import (
	"context"
	"errors"
	"testing"

	"sis2_highload/internal/domain"
	"sis2_highload/internal/service"

	"sis2_highload/internal/store/storetest"
)

type counted struct{ *Store }

func (c counted) RowCount(*testing.T) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, byChannel := range c.data {
		n += len(byChannel)
	}
	return n
}

func TestStore(t *testing.T) {
	storetest.Run(t, func(*testing.T) storetest.Store { return counted{New()} })
}

func TestBoundedEvictsOldest(t *testing.T) {
	s := NewBounded(2)
	ctx := context.Background()
	for _, id := range []string{"a", "b", "c"} {
		item := service.NewEvent{Event: domain.Event{ID: id}, Channels: []domain.Channel{domain.ChannelPush}}
		if _, err := s.CreateDeliveries(ctx, []service.NewEvent{item}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.GetDeliveries(ctx, "a"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("oldest event still present: err=%v", err)
	}
	for _, id := range []string{"b", "c"} {
		if _, err := s.GetDeliveries(ctx, id); err != nil {
			t.Fatalf("recent event %s evicted: %v", id, err)
		}
	}
	if got := (counted{s}).RowCount(t); got != 2 {
		t.Fatalf("rows = %d, want 2", got)
	}
}

func TestBoundedPassesSuite(t *testing.T) {
	storetest.Run(t, func(*testing.T) storetest.Store { return counted{NewBounded(1000)} })
}
