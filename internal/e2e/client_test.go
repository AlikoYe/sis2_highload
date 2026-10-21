package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	pb "sis2_highload/gen/notifications/v1"
	"sis2_highload/internal/client"
	"sis2_highload/internal/domain"
	"sis2_highload/internal/faults"
	"sis2_highload/internal/grpcapi"
	"sis2_highload/internal/httpapi"
	"sis2_highload/internal/retry"
	"sis2_highload/internal/store/memory"
	"sis2_highload/internal/store/postgres"
)

type protocol struct {
	name string
	dial func(*harness, client.Options) (client.Client, error)
}

var protocols = []protocol{
	{"gRPC", func(h *harness, o client.Options) (client.Client, error) {
		return client.NewGRPC("passthrough:///bufnet", o, h.grpcDialer)
	}},
	{"REST-HTTP1.1", func(h *harness, o client.Options) (client.Client, error) { return client.NewREST(h.httpURL, o), nil }},
	{"REST-h2c", func(h *harness, o client.Options) (client.Client, error) { return client.NewRESTH2C(h.h2cURL, o), nil }},
}

func (p protocol) client(t *testing.T, h *harness, o client.Options) client.Client {
	t.Helper()
	c, err := p.dial(h, o)
	if err != nil {
		t.Fatalf("%s client: %v", p.name, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

type backend struct {
	name string
	open func(t *testing.T) (store, func(t *testing.T, ids []string) int)
}

func memoryBackend() backend {
	return backend{"memory", func(t *testing.T) (store, func(*testing.T, []string) int) {
		st := memory.New()
		return st, func(t *testing.T, ids []string) int {
			n := 0
			for _, id := range ids {
				ds, err := st.GetDeliveries(context.Background(), id)
				if err != nil {
					t.Fatalf("GetDeliveries(%s): %v", id, err)
				}
				n += len(ds)
			}
			return n
		}
	}}
}

func postgresBackend() backend {
	return backend{"postgres", func(t *testing.T) (store, func(*testing.T, []string) int) {
		ctx := context.Background()
		pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		t.Cleanup(pool.Close)
		if _, err := pool.Exec(ctx, "TRUNCATE deliveries RESTART IDENTITY"); err != nil {
			t.Fatalf("truncate: %v", err)
		}
		return postgres.New(pool), func(t *testing.T, _ []string) int {
			var n int
			if err := pool.QueryRow(ctx, "SELECT count(*) FROM deliveries").Scan(&n); err != nil {
				t.Fatalf("count: %v", err)
			}
			return n
		}
	}}
}

func backends() []backend {
	bs := []backend{memoryBackend()}
	if os.Getenv("DATABASE_URL") != "" {
		bs = append(bs, postgresBackend())
	}
	return bs
}

// MaxAttempts 10, not 5: with p=0.3 per attempt, 5 attempts leave 0.3^5 = 0.24% of events
// failing, i.e. a ~39% chance that one of 200 events fails. 10 attempts make that ~0.1%.
var faultyOpts = client.Options{
	CallTimeout: 2 * time.Second,
	Policy:      retry.Policy{MaxAttempts: 10, BaseDelay: time.Millisecond, MaxDelay: 20 * time.Millisecond},
}

func clientEvent(typ string) domain.Event {
	return domain.Event{ID: client.NewEventID(), Type: typ, UserID: 7, Payload: map[string]string{"order_id": "A-1"}}
}

// publishAll publishes n fresh order.shipped events with 16 concurrent workers.
func publishAll(t *testing.T, c client.Client, n int) ([]string, map[string]domain.Result) {
	t.Helper()
	events := make([]domain.Event, n)
	for i := range events {
		events[i] = clientEvent("order.shipped")
	}
	results := make([]domain.Result, n)
	errs := make([]error, n)
	next := make(chan int)
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for i := range next {
				results[i], errs[i] = c.Publish(context.Background(), events[i])
			}
		})
	}
	for i := range events {
		next <- i
	}
	close(next)
	wg.Wait()

	ids := make([]string, n)
	byID := make(map[string]domain.Result, n)
	for i, e := range events {
		if errs[i] != nil {
			t.Fatalf("event %d not accepted: %v", i, errs[i])
		}
		if results[i].EventID != e.ID || len(results[i].Deliveries) != 2 {
			t.Fatalf("event %d: unexpected result %+v", i, results[i])
		}
		ids[i] = e.ID
		byID[e.ID] = results[i]
	}
	return ids, byID
}

func logStats(t *testing.T, s client.Stats, rows int) {
	t.Logf("calls=%d attempts=%d retries=%d budget_exhausted=%d rows=%d",
		s.Calls, s.Attempts, s.Retries, s.BudgetExhausted, rows)
}

func TestClientRetriesFailBefore(t *testing.T) {
	for _, p := range protocols {
		t.Run(p.name, func(t *testing.T) {
			st, rows := memoryBackend().open(t)
			h := newFaultyHarness(t, st, faults.Config{FailBefore: 0.3})
			c := p.client(t, h, faultyOpts)

			ids, _ := publishAll(t, c, 200)
			s, n := c.Stats(), rows(t, ids)
			logStats(t, s, n)
			if n != 400 {
				t.Fatalf("rows = %d, want 400 (one per event and channel)", n)
			}
			if s.Retries == 0 {
				t.Fatal("no retries with FailBefore=0.3")
			}
		})
	}
}

func TestClientRetriesLostResponses(t *testing.T) {
	for _, b := range backends() {
		for _, p := range protocols {
			t.Run(b.name+"/"+p.name, func(t *testing.T) {
				st, rows := b.open(t)
				h := newFaultyHarness(t, st, faults.Config{FailAfter: 0.3})
				c := p.client(t, h, faultyOpts)

				ids, results := publishAll(t, c, 200)
				s, n := c.Stats(), rows(t, ids)
				logStats(t, s, n)
				if n != 400 {
					t.Fatalf("rows = %d, want 400: retries after lost responses created duplicates", n)
				}
				if s.Retries == 0 {
					t.Fatal("no retries with FailAfter=0.3")
				}
				for _, id := range ids {
					got, err := c.GetStatus(context.Background(), id)
					if err != nil {
						t.Fatalf("GetStatus(%s): %v", id, err)
					}
					if !reflect.DeepEqual(got, results[id]) {
						t.Fatalf("GetStatus(%s) = %+v, publish returned %+v", id, got, results[id])
					}
				}
			})
		}
	}
}

func TestClientInvalidEventNotRetried(t *testing.T) {
	for _, p := range protocols {
		t.Run(p.name, func(t *testing.T) {
			h := newHarness(t, memory.New())
			c := p.client(t, h, faultyOpts)
			ev := clientEvent("order.shipped")
			ev.UserID = 0
			_, err := c.Publish(context.Background(), ev)
			if !errors.Is(err, domain.ErrInvalidArgument) {
				t.Fatalf("err = %v, want ErrInvalidArgument", err)
			}
			if s := c.Stats(); s.Attempts != 1 {
				t.Fatalf("attempts = %d, want 1", s.Attempts)
			}
		})
	}
}

func TestClientRetryBudget(t *testing.T) {
	const calls = 100
	for _, p := range protocols {
		t.Run(p.name, func(t *testing.T) {
			h := newFaultyHarness(t, memory.New(), faults.Config{FailBefore: 1})
			c := p.client(t, h, client.Options{
				Policy: retry.Policy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond},
				Budget: retry.DefaultBudget(),
			})
			for i := range calls {
				if _, err := c.Publish(context.Background(), clientEvent("order.shipped")); !errors.Is(err, domain.ErrUnavailable) {
					t.Fatalf("call %d: err = %v, want ErrUnavailable", i, err)
				}
			}
			s := c.Stats()
			logStats(t, s, 0)
			if limit := int64(10 + calls*0.1); s.Retries > limit {
				t.Fatalf("retries = %d, want <= %d", s.Retries, limit)
			}
			if s.BudgetExhausted == 0 {
				t.Fatal("budget never exhausted")
			}
		})
	}
}

func TestClientDeadlineNotRetried(t *testing.T) {
	for _, p := range protocols {
		t.Run(p.name, func(t *testing.T) {
			h := newFaultyHarness(t, memory.New(), faults.Config{Latency: 200 * time.Millisecond})
			c := p.client(t, h, client.Options{
				CallTimeout: 100 * time.Millisecond,
				Policy:      retry.Policy{MaxAttempts: 5, BaseDelay: time.Millisecond},
			})
			start := time.Now()
			_, err := c.Publish(context.Background(), clientEvent("order.shipped"))
			took := time.Since(start)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("err = %v, want DeadlineExceeded", err)
			}
			if s := c.Stats(); s.Attempts != 1 {
				t.Fatalf("attempts = %d, want 1", s.Attempts)
			}
			if took > 150*time.Millisecond {
				t.Fatalf("took %v, want ~100ms", took)
			}
			t.Logf("returned after %v", took.Round(time.Millisecond))
		})
	}
}

// arrivals records when each request reached a fake server.
type arrivals struct {
	mu    sync.Mutex
	times []time.Time
}

func (a *arrivals) add() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.times = append(a.times, time.Now())
	return len(a.times)
}

func (a *arrivals) gap(t *testing.T) time.Duration {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.times) != 2 {
		t.Fatalf("server saw %d requests, want 2", len(a.times))
	}
	return a.times[1].Sub(a.times[0])
}

type retryAfterGRPC struct {
	pb.UnimplementedNotificationServiceServer
	arrivals
}

func (s *retryAfterGRPC) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	if s.add() == 1 {
		_ = grpc.SetTrailer(ctx, metadata.Pairs("retry-after-ms", "300"))
		return nil, status.Error(codes.Unavailable, "busy")
	}
	return grpcapi.PublishResponse(domain.Result{EventID: req.GetEvent().GetEventId()}), nil
}

func retryAfterREST(a *arrivals) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var ev httpapi.Event
		_ = json.NewDecoder(r.Body).Decode(&ev)
		w.Header().Set("Content-Type", "application/json")
		if a.add() == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(httpapi.ErrorResponse{Error: httpapi.ErrorBody{Code: "UNAVAILABLE", Message: "busy"}})
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(httpapi.Result{EventID: ev.EventID, Deliveries: []httpapi.Delivery{}})
	})
}

func TestClientHonoursRetryAfter(t *testing.T) {
	opts := client.Options{
		CallTimeout: 3 * time.Second,
		Policy:      retry.Policy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond},
	}
	for _, p := range protocols {
		t.Run(p.name, func(t *testing.T) {
			var a *arrivals
			var h harness
			hint := time.Second
			switch p.name {
			case "gRPC":
				hint = 300 * time.Millisecond
				fake := &retryAfterGRPC{}
				a = &fake.arrivals
				srv := grpc.NewServer()
				pb.RegisterNotificationServiceServer(srv, fake)
				lis := bufconn.Listen(1 << 20)
				go func() { _ = srv.Serve(lis) }()
				t.Cleanup(srv.Stop)
				h.grpcDialer = grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) })
			case "REST-HTTP1.1":
				a = &arrivals{}
				ts := httptest.NewServer(retryAfterREST(a))
				t.Cleanup(ts.Close)
				h.httpURL = ts.URL
			case "REST-h2c":
				a = &arrivals{}
				ts := httptest.NewUnstartedServer(retryAfterREST(a))
				ts.Config.Protocols = new(http.Protocols)
				ts.Config.Protocols.SetUnencryptedHTTP2(true)
				ts.Start()
				t.Cleanup(ts.Close)
				h.h2cURL = ts.URL
			}
			c := p.client(t, &h, opts)
			if _, err := c.Publish(context.Background(), clientEvent("order.shipped")); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			if gap := a.gap(t); gap < hint {
				t.Fatalf("retried after %v, want >= %v", gap, hint)
			} else {
				t.Logf("hint %v, retried after %v", hint, gap.Round(time.Millisecond))
			}
		})
	}
}
