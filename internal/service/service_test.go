package service_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"sis2_highload/internal/domain"
	"sis2_highload/internal/service"
	"sis2_highload/internal/store/memory"
)

type spyStore struct {
	service.Store
	calls int
	items []service.NewEvent
	err   error
}

func (s *spyStore) CreateDeliveries(ctx context.Context, items []service.NewEvent) ([]domain.Result, error) {
	s.calls++
	s.items = items
	if s.err != nil {
		return nil, s.err
	}
	return s.Store.CreateDeliveries(ctx, items)
}

func (s *spyStore) GetDeliveries(ctx context.Context, id string) ([]domain.Delivery, error) {
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	return s.Store.GetDeliveries(ctx, id)
}

func newService() (*service.Service, *spyStore) {
	spy := &spyStore{Store: memory.New()}
	return service.New(spy), spy
}

func validEvent() domain.Event {
	return domain.Event{ID: uuid.NewString(), Type: "order.shipped", UserID: 1, Payload: map[string]string{"k": "v"}}
}

func payloadWith(keys, valueLen int) map[string]string {
	p := make(map[string]string, keys)
	for i := range keys {
		p[fmt.Sprintf("k%02d", i)] = strings.Repeat("x", valueLen)
	}
	return p
}

func TestPublishValidation(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*domain.Event)
		wantErr bool
	}{
		{"valid", func(*domain.Event) {}, false},
		{"empty event_id", func(e *domain.Event) { e.ID = "" }, true},
		{"malformed event_id", func(e *domain.Event) { e.ID = "not-a-uuid" }, true},
		{"empty type", func(e *domain.Event) { e.Type = "" }, true},
		{"unknown type", func(e *domain.Event) { e.Type = "order.created" }, true},
		{"zero user_id", func(e *domain.Event) { e.UserID = 0 }, true},
		{"negative user_id", func(e *domain.Event) { e.UserID = -5 }, true},
		{"nil payload", func(e *domain.Event) { e.Payload = nil }, false},
		{"32 keys", func(e *domain.Event) { e.Payload = payloadWith(32, 1) }, false},
		{"33 keys", func(e *domain.Event) { e.Payload = payloadWith(33, 1) }, true},
		// 2 keys * (3-byte key + 2045-byte value) = 4096 bytes
		{"4096 bytes", func(e *domain.Event) { e.Payload = payloadWith(2, 2045) }, false},
		{"4097 bytes", func(e *domain.Event) { e.Payload = payloadWith(2, 2045); e.Payload["k"] = "" }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, spy := newService()
			ev := validEvent()
			tt.mutate(&ev)
			_, err := svc.Publish(context.Background(), ev)
			if tt.wantErr {
				if !errors.Is(err, domain.ErrInvalidArgument) {
					t.Fatalf("err = %v, want ErrInvalidArgument", err)
				}
				if spy.calls != 0 {
					t.Fatalf("store called %d times on invalid input", spy.calls)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestPublishChannelsByType(t *testing.T) {
	tests := []struct {
		typ  string
		want []domain.Channel
	}{
		{"order.shipped", []domain.Channel{domain.ChannelEmail, domain.ChannelPush}},
		{"payment.failed", []domain.Channel{domain.ChannelEmail, domain.ChannelPush}},
		{"auth.login_code", []domain.Channel{domain.ChannelPush}},
	}
	for _, tt := range tests {
		t.Run(tt.typ, func(t *testing.T) {
			svc, _ := newService()
			ev := validEvent()
			ev.Type = tt.typ
			res, err := svc.Publish(context.Background(), ev)
			if err != nil {
				t.Fatalf("Publish: %v", err)
			}
			var got []domain.Channel
			for _, d := range res.Deliveries {
				if d.Status != domain.StatusQueued || d.Attempts != 0 || d.EventID != ev.ID {
					t.Fatalf("unexpected delivery %+v", d)
				}
				got = append(got, d.Channel)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("channels = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPublishNormalizes(t *testing.T) {
	svc, spy := newService()
	ev := validEvent()
	id := ev.ID
	ev.ID = strings.ToUpper(id)

	before := time.Now()
	res, err := svc.Publish(context.Background(), ev)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if res.EventID != id {
		t.Fatalf("event_id = %q, want canonical %q", res.EventID, id)
	}
	at := spy.items[0].Event.OccurredAt
	if at.Before(before) || at.Location() != time.UTC {
		t.Fatalf("occurred_at = %v, want now in UTC", at)
	}
}

func TestPublishIsIdempotent(t *testing.T) {
	svc, _ := newService()
	ev := validEvent()
	first, err := svc.Publish(context.Background(), ev)
	if err != nil {
		t.Fatalf("first Publish: %v", err)
	}
	second, err := svc.Publish(context.Background(), ev)
	if err != nil {
		t.Fatalf("second Publish: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("second = %+v, want %+v", second, first)
	}
}

func TestPublishBatch(t *testing.T) {
	events := func(n int) []domain.Event {
		evs := make([]domain.Event, n)
		for i := range evs {
			evs[i] = validEvent()
		}
		return evs
	}
	withInvalid := events(3)
	withInvalid[1].UserID = 0

	tests := []struct {
		name    string
		events  []domain.Event
		wantErr bool
	}{
		{"empty", nil, true},
		{"one", events(1), false},
		{"100", events(100), false},
		{"101", events(101), true},
		{"one invalid event", withInvalid, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, spy := newService()
			res, err := svc.PublishBatch(context.Background(), tt.events)
			if tt.wantErr {
				if !errors.Is(err, domain.ErrInvalidArgument) {
					t.Fatalf("err = %v, want ErrInvalidArgument", err)
				}
				if spy.calls != 0 {
					t.Fatalf("store called %d times on invalid input", spy.calls)
				}
				return
			}
			if err != nil {
				t.Fatalf("PublishBatch: %v", err)
			}
			for i, r := range res {
				if r.EventID != tt.events[i].ID {
					t.Fatalf("results[%d].EventID = %s, want %s (input order)", i, r.EventID, tt.events[i].ID)
				}
			}
		})
	}
}

func TestPublishBatchDuplicateIDs(t *testing.T) {
	svc, _ := newService()
	a, b := validEvent(), validEvent()
	res, err := svc.PublishBatch(context.Background(), []domain.Event{a, b, a})
	if err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	if !reflect.DeepEqual(res[0], res[2]) {
		t.Fatalf("duplicate results differ: %+v vs %+v", res[0], res[2])
	}
	got, err := svc.GetStatus(context.Background(), a.ID)
	if err != nil {
		t.Fatalf("GetStatus: %v", err)
	}
	if !reflect.DeepEqual(got, res[0]) {
		t.Fatalf("status = %+v, want %+v", got, res[0])
	}
}

func TestGetStatus(t *testing.T) {
	svc, _ := newService()
	if _, err := svc.GetStatus(context.Background(), "nope"); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("malformed id: err = %v, want ErrInvalidArgument", err)
	}
	if _, err := svc.GetStatus(context.Background(), uuid.NewString()); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("unknown id: err = %v, want ErrNotFound", err)
	}
}

func TestCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	svc, spy := newService()

	calls := map[string]func() error{
		"Publish": func() error { _, err := svc.Publish(ctx, validEvent()); return err },
		"PublishBatch": func() error {
			_, err := svc.PublishBatch(ctx, []domain.Event{validEvent()})
			return err
		},
		"GetStatus": func() error { _, err := svc.GetStatus(ctx, uuid.NewString()); return err },
	}
	for name, call := range calls {
		if err := call(); !errors.Is(err, context.Canceled) {
			t.Errorf("%s: err = %v, want context.Canceled", name, err)
		}
	}
	if spy.calls != 0 {
		t.Fatalf("store called %d times with cancelled ctx", spy.calls)
	}
}

func TestStoreErrorsMapToUnavailable(t *testing.T) {
	cause := errors.New("connection refused")
	svc, spy := newService()
	spy.err = cause

	_, err := svc.Publish(context.Background(), validEvent())
	if !errors.Is(err, domain.ErrUnavailable) || !errors.Is(err, cause) {
		t.Fatalf("Publish err = %v, want ErrUnavailable wrapping cause", err)
	}
	_, err = svc.GetStatus(context.Background(), uuid.NewString())
	if !errors.Is(err, domain.ErrUnavailable) || !errors.Is(err, cause) {
		t.Fatalf("GetStatus err = %v, want ErrUnavailable wrapping cause", err)
	}
}
