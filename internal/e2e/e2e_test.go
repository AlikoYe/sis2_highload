package e2e

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	pb "sis2_highload/gen/notifications/v1"
	"sis2_highload/internal/domain"
	"sis2_highload/internal/store/memory"
)

func (h *harness) publishREST(t *testing.T, id, typ string) result {
	t.Helper()
	code, raw := h.do(t, http.MethodPost, "/v1/events", restEvent(id, typ), nil)
	if code != http.StatusAccepted {
		t.Fatalf("REST publish: status %d, body %s", code, raw)
	}
	return decode[restResult](t, raw).neutral()
}

func (h *harness) publishGRPC(t *testing.T, id, typ string) result {
	t.Helper()
	resp, err := h.grpc.Publish(context.Background(), &pb.PublishRequest{Event: grpcEvent(id, typ)})
	if err != nil {
		t.Fatalf("gRPC publish: %v", err)
	}
	return fromProto(resp.GetEventId(), resp.GetDeliveries())
}

func TestParity(t *testing.T) {
	h := newHarness(t, memory.New())
	for _, typ := range []string{"order.shipped", "payment.failed", "auth.login_code"} {
		t.Run(typ, func(t *testing.T) {
			viaGRPC := h.publishGRPC(t, newID(), typ)
			viaREST := h.publishREST(t, newID(), typ)
			if len(viaGRPC.Deliveries) == 0 || !reflect.DeepEqual(viaGRPC.Deliveries, viaREST.Deliveries) {
				t.Fatalf("gRPC %+v != REST %+v", viaGRPC.Deliveries, viaREST.Deliveries)
			}
		})
	}
}

func TestCrossProtocolIdempotency(t *testing.T) {
	h := newHarness(t, memory.New())
	tests := []struct {
		name          string
		first, second func(*testing.T, string, string) result
	}{
		{"REST then gRPC", h.publishREST, h.publishGRPC},
		{"gRPC then REST", h.publishGRPC, h.publishREST},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := newID()
			first := tt.first(t, id, "order.shipped")
			second := tt.second(t, id, "order.shipped")
			if !reflect.DeepEqual(first, second) {
				t.Fatalf("first %+v != second %+v", first, second)
			}
			st, err := h.grpc.GetStatus(context.Background(), &pb.GetStatusRequest{EventId: id})
			if err != nil {
				t.Fatalf("GetStatus: %v", err)
			}
			if got := fromProto(st.GetEventId(), st.GetDeliveries()); !reflect.DeepEqual(got, first) {
				t.Fatalf("stored %+v, want one row per channel %+v", got, first)
			}
		})
	}
}

const secret = "postgres://notif:hunter2@db"

func TestErrorMapping(t *testing.T) {
	tests := []struct {
		name     string
		store    func(*fakeStore)
		grpc     codes.Code
		http     int
		code     string
		internal bool // message must not leak the cause
	}{
		{"invalid argument", func(f *fakeStore) { f.err = fmt.Errorf("%w: bad", domain.ErrInvalidArgument) }, codes.InvalidArgument, 400, "INVALID_ARGUMENT", false},
		{"not found", func(f *fakeStore) { f.err = domain.ErrNotFound }, codes.NotFound, 404, "NOT_FOUND", false},
		{"overloaded", func(f *fakeStore) { f.err = domain.ErrOverloaded }, codes.ResourceExhausted, 429, "RESOURCE_EXHAUSTED", false},
		{"unavailable", func(f *fakeStore) { f.err = fmt.Errorf("%w: %s down", domain.ErrUnavailable, secret) }, codes.Unavailable, 503, "UNAVAILABLE", true},
		{"raw store error", func(f *fakeStore) { f.err = errors.New("dial " + secret) }, codes.Unavailable, 503, "UNAVAILABLE", true},
		{"deadline exceeded", func(f *fakeStore) { f.err = context.DeadlineExceeded }, codes.DeadlineExceeded, 504, "DEADLINE_EXCEEDED", true},
		{"canceled", func(f *fakeStore) { f.err = context.Canceled }, codes.Canceled, 499, "CANCELED", true},
		{"panic is internal", func(f *fakeStore) { f.panic = true }, codes.Internal, 500, "INTERNAL", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := newFakeStore()
			tt.store(fake)
			h := newHarness(t, fake)
			id := newID()

			t.Run("gRPC", func(t *testing.T) {
				_, err := h.grpc.Publish(context.Background(), &pb.PublishRequest{Event: grpcEvent(id, "order.shipped")})
				st := status.Convert(err)
				if st.Code() != tt.grpc {
					t.Fatalf("code = %v (%q), want %v", st.Code(), st.Message(), tt.grpc)
				}
				if tt.internal && strings.Contains(st.Message(), "hunter2") {
					t.Fatalf("message leaks cause: %q", st.Message())
				}
			})
			t.Run("REST", func(t *testing.T) {
				code, raw := h.do(t, http.MethodPost, "/v1/events", restEvent(id, "order.shipped"), nil)
				body := decode[restError](t, raw)
				if code != tt.http || body.Error.Code != tt.code {
					t.Fatalf("got %d %s, want %d %s", code, body.Error.Code, tt.http, tt.code)
				}
				if tt.internal && strings.Contains(body.Error.Message, "hunter2") {
					t.Fatalf("message leaks cause: %q", body.Error.Message)
				}
			})
		})
	}
}

func TestRESTBadRequests(t *testing.T) {
	h := newHarness(t, memory.New())
	tests := []struct {
		name string
		body string
		msg  string
	}{
		{"malformed JSON", `{"event_id":`, "malformed JSON"},
		{"wrong field type", `{"event_id":"x","user_id":"seven"}`, "malformed JSON"},
		{"validation failure", fmt.Sprintf(`{"event_id":%q,"type":"nope","user_id":1}`, newID()), "unknown event type"},
		{"body over 1 MiB", fmt.Sprintf(`{"event_id":%q,"type":"order.shipped","user_id":1,"payload":{"k":%q}}`, newID(), strings.Repeat("x", 1<<20)), "too large"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, raw := h.do(t, http.MethodPost, "/v1/events", tt.body, nil)
			body := decode[restError](t, raw)
			if code != 400 || body.Error.Code != "INVALID_ARGUMENT" || !strings.Contains(body.Error.Message, tt.msg) {
				t.Fatalf("got %d %s, want 400 INVALID_ARGUMENT containing %q", code, raw, tt.msg)
			}
		})
	}
	t.Run("gRPC nil event", func(t *testing.T) {
		_, err := h.grpc.Publish(context.Background(), &pb.PublishRequest{})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("err = %v, want InvalidArgument", err)
		}
	})
}

func TestDeadlines(t *testing.T) {
	waitObserved := func(t *testing.T, fake *fakeStore) {
		t.Helper()
		select {
		case <-fake.done:
		case <-time.After(2 * time.Second):
			t.Fatal("store never observed ctx cancellation")
		}
	}

	t.Run("gRPC", func(t *testing.T) {
		fake := newFakeStore()
		fake.block = true
		h := newHarness(t, fake)
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_, err := h.grpc.Publish(ctx, &pb.PublishRequest{Event: grpcEvent(newID(), "order.shipped")})
		if status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("err = %v, want DeadlineExceeded", err)
		}
		waitObserved(t, fake)
	})
	t.Run("REST", func(t *testing.T) {
		fake := newFakeStore()
		fake.block = true
		h := newHarness(t, fake)
		start := time.Now()
		code, raw := h.do(t, http.MethodPost, "/v1/events", restEvent(newID(), "order.shipped"),
			http.Header{"X-Request-Timeout-Ms": {"50"}})
		if code != http.StatusGatewayTimeout || decode[restError](t, raw).Error.Code != "DEADLINE_EXCEEDED" {
			t.Fatalf("got %d %s, want 504 DEADLINE_EXCEEDED", code, raw)
		}
		if took := time.Since(start); took > time.Second {
			t.Fatalf("took %v, header timeout not applied", took)
		}
		waitObserved(t, fake)
	})
}

func TestBatch(t *testing.T) {
	h := newHarness(t, memory.New())

	ids := []string{newID(), newID(), newID()}
	types := []string{"order.shipped", "auth.login_code", "payment.failed"}

	t.Run("gRPC", func(t *testing.T) {
		req := &pb.PublishBatchRequest{}
		for i := range ids {
			req.Events = append(req.Events, grpcEvent(ids[i], types[i]))
		}
		resp, err := h.grpc.PublishBatch(context.Background(), req)
		if err != nil {
			t.Fatalf("PublishBatch: %v", err)
		}
		if len(resp.GetResults()) != len(ids) {
			t.Fatalf("got %d results", len(resp.GetResults()))
		}
		for i, r := range resp.GetResults() {
			if r.GetEventId() != ids[i] {
				t.Fatalf("results[%d] = %s, want %s", i, r.GetEventId(), ids[i])
			}
		}
	})
	t.Run("REST same events is idempotent", func(t *testing.T) {
		var events []map[string]any
		for i := range ids {
			events = append(events, restEvent(ids[i], types[i]))
		}
		code, raw := h.do(t, http.MethodPost, "/v1/events/batch", map[string]any{"events": events}, nil)
		if code != http.StatusAccepted {
			t.Fatalf("status %d: %s", code, raw)
		}
		resp := decode[struct {
			Results []restResult `json:"results"`
		}](t, raw)
		for i, r := range resp.Results {
			st, err := h.grpc.GetStatus(context.Background(), &pb.GetStatusRequest{EventId: ids[i]})
			if err != nil {
				t.Fatalf("GetStatus: %v", err)
			}
			if want := fromProto(st.GetEventId(), st.GetDeliveries()); !reflect.DeepEqual(r.neutral(), want) {
				t.Fatalf("results[%d] = %+v, want %+v", i, r.neutral(), want)
			}
		}
	})

	t.Run("over 100 events rejected", func(t *testing.T) {
		req := &pb.PublishBatchRequest{}
		var events []map[string]any
		for range 101 {
			id := newID()
			req.Events = append(req.Events, grpcEvent(id, "order.shipped"))
			events = append(events, restEvent(id, "order.shipped"))
		}
		if _, err := h.grpc.PublishBatch(context.Background(), req); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("gRPC err = %v, want InvalidArgument", err)
		}
		if code, raw := h.do(t, http.MethodPost, "/v1/events/batch", map[string]any{"events": events}, nil); code != 400 {
			t.Fatalf("REST status %d: %s, want 400", code, raw)
		}
	})
}

func TestGetStatusREST(t *testing.T) {
	h := newHarness(t, memory.New())
	id := newID()
	want := h.publishGRPC(t, id, "order.shipped")

	code, raw := h.do(t, http.MethodGet, "/v1/events/"+id, nil, nil)
	if code != http.StatusOK {
		t.Fatalf("status %d: %s", code, raw)
	}
	if got := decode[restResult](t, raw).neutral(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	if code, _ := h.do(t, http.MethodGet, "/v1/events/"+newID(), nil, nil); code != http.StatusNotFound {
		t.Fatalf("unknown id: status %d, want 404", code)
	}
}

func TestHealth(t *testing.T) {
	h := newHarness(t, memory.New())
	for _, path := range []string{"/healthz", "/readyz"} {
		if code, _ := h.do(t, http.MethodGet, path, nil, nil); code != http.StatusOK {
			t.Fatalf("%s = %d, want 200", path, code)
		}
	}
	resp, err := h.health.Check(context.Background(), &healthpb.HealthCheckRequest{})
	if err != nil || resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("gRPC health = %v, %v; want SERVING", resp.GetStatus(), err)
	}

	fake := newFakeStore()
	fake.pingErr = errors.New("db down")
	down := newHarness(t, fake)
	if code, _ := down.do(t, http.MethodGet, "/readyz", nil, nil); code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz with failing ping = %d, want 503", code)
	}
	if code, _ := down.do(t, http.MethodGet, "/healthz", nil, nil); code != http.StatusOK {
		t.Fatalf("/healthz with failing ping = %d, want 200", code)
	}
}
