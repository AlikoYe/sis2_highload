// Package storetest is a shared behaviour suite for service.Store implementations.
package storetest

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"sis2_highload/internal/domain"
	"sis2_highload/internal/service"
)

// Store is a service.Store that can also report its total number of delivery rows.
type Store interface {
	service.Store
	RowCount(t *testing.T) int
}

func Run(t *testing.T, newStore func(t *testing.T) Store) {
	t.Run("CreateThenGet", func(t *testing.T) { testCreateThenGet(t, newStore(t)) })
	t.Run("CreateTwiceIsIdempotent", func(t *testing.T) { testCreateTwice(t, newStore(t)) })
	t.Run("ConcurrentDuplicates", func(t *testing.T) { testConcurrentDuplicates(t, newStore(t)) })
	t.Run("BatchWithDuplicateEventID", func(t *testing.T) { testBatchDuplicates(t, newStore(t)) })
	t.Run("GetUnknownIsNotFound", func(t *testing.T) { testGetUnknown(t, newStore(t)) })
}

func newEvent(channels ...domain.Channel) service.NewEvent {
	return service.NewEvent{
		Event: domain.Event{
			ID:         uuid.NewString(),
			Type:       "order.shipped",
			UserID:     42,
			Payload:    map[string]string{"order_id": "A-1"},
			OccurredAt: time.Now().UTC(),
		},
		Channels: channels,
	}
}

func queued(id string, channels ...domain.Channel) domain.Result {
	ds := make([]domain.Delivery, len(channels))
	for i, ch := range channels {
		ds[i] = domain.Delivery{EventID: id, Channel: ch, Status: domain.StatusQueued}
	}
	return domain.NewResult(id, ds)
}

func create(t *testing.T, s Store, items ...service.NewEvent) []domain.Result {
	t.Helper()
	res, err := s.CreateDeliveries(context.Background(), items)
	if err != nil {
		t.Fatalf("CreateDeliveries: %v", err)
	}
	if len(res) != len(items) {
		t.Fatalf("got %d results, want %d", len(res), len(items))
	}
	return res
}

func assertRows(t *testing.T, s Store, want int) {
	t.Helper()
	if got := s.RowCount(t); got != want {
		t.Fatalf("row count = %d, want %d", got, want)
	}
}

func testCreateThenGet(t *testing.T, s Store) {
	ne := newEvent(domain.ChannelPush, domain.ChannelEmail)
	want := queued(ne.Event.ID, domain.ChannelPush, domain.ChannelEmail)

	res := create(t, s, ne)
	if !reflect.DeepEqual(res[0], want) {
		t.Fatalf("create = %+v, want %+v", res[0], want)
	}
	ds, err := s.GetDeliveries(context.Background(), ne.Event.ID)
	if err != nil {
		t.Fatalf("GetDeliveries: %v", err)
	}
	if !reflect.DeepEqual(ds, want.Deliveries) {
		t.Fatalf("get = %+v, want %+v", ds, want.Deliveries)
	}
	assertRows(t, s, 2)
}

func testCreateTwice(t *testing.T, s Store) {
	ne := newEvent(domain.ChannelPush, domain.ChannelEmail)
	first := create(t, s, ne)
	second := create(t, s, ne)
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("second = %+v, want %+v", second, first)
	}
	assertRows(t, s, 2)
}

func testConcurrentDuplicates(t *testing.T, s Store) {
	const n = 50
	ne := newEvent(domain.ChannelPush, domain.ChannelEmail)
	want := queued(ne.Event.ID, domain.ChannelPush, domain.ChannelEmail)

	start := make(chan struct{})
	results := make([][]domain.Result, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			<-start
			results[i], errs[i] = s.CreateDeliveries(context.Background(), []service.NewEvent{ne})
		})
	}
	close(start)
	wg.Wait()

	for i := range n {
		if errs[i] != nil {
			t.Fatalf("goroutine %d: %v", i, errs[i])
		}
		if len(results[i]) != 1 || !reflect.DeepEqual(results[i][0], want) {
			t.Fatalf("goroutine %d: got %+v, want %+v", i, results[i], want)
		}
	}
	assertRows(t, s, 2)
}

func testBatchDuplicates(t *testing.T, s Store) {
	a := newEvent(domain.ChannelPush, domain.ChannelEmail)
	b := newEvent(domain.ChannelPush)

	res := create(t, s, a, b, a)
	if !reflect.DeepEqual(res[0], res[2]) {
		t.Fatalf("duplicate results differ: %+v vs %+v", res[0], res[2])
	}
	if want := queued(a.Event.ID, domain.ChannelPush, domain.ChannelEmail); !reflect.DeepEqual(res[0], want) {
		t.Fatalf("a = %+v, want %+v", res[0], want)
	}
	if want := queued(b.Event.ID, domain.ChannelPush); !reflect.DeepEqual(res[1], want) {
		t.Fatalf("b = %+v, want %+v", res[1], want)
	}
	assertRows(t, s, 3)
}

func testGetUnknown(t *testing.T, s Store) {
	_, err := s.GetDeliveries(context.Background(), uuid.NewString())
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}
