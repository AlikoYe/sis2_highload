// Package e2e runs the gRPC and REST adapters side by side over one service.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"

	pb "sis2_highload/gen/notifications/v1"
	"sis2_highload/internal/domain"
	"sis2_highload/internal/faults"
	"sis2_highload/internal/grpcapi"
	"sis2_highload/internal/httpapi"
	"sis2_highload/internal/service"
)

type store interface {
	service.Store
	httpapi.Pinger
}

type harness struct {
	grpc       pb.NotificationServiceClient
	health     healthpb.HealthClient
	grpcDialer grpc.DialOption
	httpURL    string // HTTP/1.1
	h2cURL     string // cleartext HTTP/2 only
}

func newHarness(t *testing.T, st store) *harness {
	return newFaultyHarness(t, st, faults.Config{})
}

// newFaultyHarness serves one service over gRPC (bufconn), REST/HTTP1.1 and REST/h2c,
// all behind the same fault injector.
func newFaultyHarness(t *testing.T, st store, fc faults.Config) *harness {
	t.Helper()
	log := slog.New(slog.DiscardHandler)
	svc := service.New(st)
	inj := faults.New(fc)

	gs, hs := grpcapi.New(svc, log, inj)
	grpcapi.SetServing(hs, true)
	lis := bufconn.Listen(1 << 20)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	dialer := grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) })
	conn, err := grpc.NewClient("passthrough:///bufnet", dialer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	handler := httpapi.New(svc, st, log, inj)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	h2c := httptest.NewUnstartedServer(handler)
	h2c.Config.Protocols = new(http.Protocols)
	h2c.Config.Protocols.SetUnencryptedHTTP2(true)
	h2c.Start()
	t.Cleanup(h2c.Close)

	return &harness{
		grpc:       pb.NewNotificationServiceClient(conn),
		health:     healthpb.NewHealthClient(conn),
		grpcDialer: dialer,
		httpURL:    ts.URL,
		h2cURL:     h2c.URL,
	}
}

// delivery is the protocol-neutral view used to compare responses.
type delivery struct {
	Channel  string
	Status   string
	Attempts int32
}

type result struct {
	EventID    string
	Deliveries []delivery
}

func fromProto(eventID string, ds []*pb.Delivery) result {
	r := result{EventID: eventID}
	for _, d := range ds {
		r.Deliveries = append(r.Deliveries, delivery{
			Channel:  strings.TrimPrefix(d.GetChannel().String(), "CHANNEL_"),
			Status:   strings.TrimPrefix(d.GetStatus().String(), "DELIVERY_STATUS_"),
			Attempts: d.GetAttempts(),
		})
	}
	return r
}

type restDelivery struct {
	Channel  string `json:"channel"`
	Status   string `json:"status"`
	Attempts int32  `json:"attempts"`
}

type restResult struct {
	EventID    string         `json:"event_id"`
	Deliveries []restDelivery `json:"deliveries"`
}

func (r restResult) neutral() result {
	out := result{EventID: r.EventID}
	for _, d := range r.Deliveries {
		out.Deliveries = append(out.Deliveries, delivery(d))
	}
	return out
}

type restError struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func (h *harness) do(t *testing.T, method, path string, body any, header http.Header) (int, []byte) {
	t.Helper()
	var r io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		r = strings.NewReader(b)
	default:
		raw, err := json.Marshal(b)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		r = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, h.httpURL+path, r)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	for k, v := range header {
		req.Header[k] = v
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, raw
}

func decode[T any](t *testing.T, raw []byte) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	return v
}

func restEvent(id, typ string) map[string]any {
	return map[string]any{"event_id": id, "type": typ, "user_id": 7, "payload": map[string]string{"order_id": "A-1"}}
}

func grpcEvent(id, typ string) *pb.Event {
	return &pb.Event{EventId: id, Type: typ, UserId: 7, Payload: map[string]string{"order_id": "A-1"}}
}

// fakeStore returns a fixed error, panics, or blocks until the request ctx ends.
type fakeStore struct {
	err     error
	panic   bool
	block   bool
	pingErr error

	once sync.Once
	done chan struct{} // closed once a blocked call observes ctx cancellation
}

func newFakeStore() *fakeStore { return &fakeStore{done: make(chan struct{})} }

func (f *fakeStore) call(ctx context.Context) error {
	if f.panic {
		panic("fake store exploded")
	}
	if f.block {
		<-ctx.Done()
		f.once.Do(func() { close(f.done) })
		return ctx.Err()
	}
	return f.err
}

func (f *fakeStore) CreateDeliveries(ctx context.Context, _ []service.NewEvent) ([]domain.Result, error) {
	return nil, f.call(ctx)
}

func (f *fakeStore) GetDeliveries(ctx context.Context, _ string) ([]domain.Delivery, error) {
	return nil, f.call(ctx)
}

func (f *fakeStore) Ping(context.Context) error { return f.pingErr }

func newID() string { return uuid.NewString() }
