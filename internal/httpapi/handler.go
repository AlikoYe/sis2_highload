package httpapi

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"strconv"
	"time"

	"sis2_highload/internal/apierr"
	"sis2_highload/internal/domain"
	"sis2_highload/internal/faults"
	"sis2_highload/internal/service"
)

const (
	MaxBodyBytes   = 1 << 20
	TimeoutHeader  = "X-Request-Timeout-Ms"
	DefaultTimeout = 2 * time.Second
	MaxTimeout     = 5 * time.Second
)

type Pinger interface {
	Ping(ctx context.Context) error
}

type handler struct {
	svc    *service.Service
	ready  Pinger
	log    *slog.Logger
	faults *faults.Injector
}

// New returns the REST handler. injector may be nil; it applies to /v1 routes only.
func New(svc *service.Service, ready Pinger, log *slog.Logger, injector *faults.Injector) http.Handler {
	h := &handler{svc: svc, ready: ready, log: log, faults: injector}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/events", h.withFaults(h.publish))
	mux.HandleFunc("POST /v1/events/batch", h.withFaults(h.publishBatch))
	mux.HandleFunc("GET /v1/events/{id}", h.withFaults(h.getStatus))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", h.readyz)
	return h.recover(h.withTimeout(mux))
}

func (h *handler) publish(w http.ResponseWriter, r *http.Request) {
	var req Event
	if !h.decodeJSON(w, r, &req) {
		return
	}
	result, err := h.svc.Publish(r.Context(), req.ToDomain())
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusAccepted, ResultFromDomain(result))
}

func (h *handler) publishBatch(w http.ResponseWriter, r *http.Request) {
	var req BatchRequest
	if !h.decodeJSON(w, r, &req) {
		return
	}
	results, err := h.svc.PublishBatch(r.Context(), req.ToDomain())
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusAccepted, BatchResponseFromDomain(results))
}

func (h *handler) getStatus(w http.ResponseWriter, r *http.Request) {
	result, err := h.svc.GetStatus(r.Context(), r.PathValue("id"))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, ResultFromDomain(result))
}

func (h *handler) readyz(w http.ResponseWriter, r *http.Request) {
	if err := h.ready.Ping(r.Context()); err != nil {
		h.log.WarnContext(r.Context(), "readiness check failed", "err", err)
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *handler) decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		h.writeError(w, r, fmt.Errorf("%w: malformed JSON body: %v", domain.ErrInvalidArgument, err))
		return false
	}
	return true
}

// withTimeout derives the request deadline from X-Request-Timeout-Ms (default 2s, capped at 5s).
func (h *handler) withTimeout(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		timeout := DefaultTimeout
		if header := r.Header.Get(TimeoutHeader); header != "" {
			ms, err := strconv.Atoi(header)
			if err != nil || ms <= 0 {
				h.writeError(w, r, fmt.Errorf("%w: %s must be a positive integer", domain.ErrInvalidArgument, TimeoutHeader))
				return
			}
			timeout = min(time.Duration(ms)*time.Millisecond, MaxTimeout)
		}
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (h *handler) recover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				if recovered == http.ErrAbortHandler {
					panic(recovered)
				}
				h.writeError(w, r, fmt.Errorf("panic in %s %s: %v", r.Method, r.URL.Path, recovered))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// withFaults buffers the response so an injected "lost response" can replace it after
// the service has committed. Injected errors are not logged, like in the gRPC interceptor.
func (h *handler) withFaults(next http.HandlerFunc) http.HandlerFunc {
	if h.faults == nil {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if err := h.faults.Before(r.Context()); err != nil {
			respondError(w, err)
			return
		}
		buffered := &bufferedWriter{header: make(http.Header)}
		next(buffered, r)
		succeeded := buffered.status < http.StatusBadRequest
		if succeeded && h.faults.LoseResponse() {
			respondError(w, faults.ErrLostResponse)
			return
		}
		buffered.flushTo(w)
	}
}

func (h *handler) writeError(w http.ResponseWriter, r *http.Request, err error) {
	if mapping := apierr.Of(err); mapping.Log {
		h.log.ErrorContext(r.Context(), "http request failed",
			"code", mapping.Code, "method", r.Method, "path", r.URL.Path, "err", err)
	}
	respondError(w, err)
}

func respondError(w http.ResponseWriter, err error) {
	mapping := apierr.Of(err)
	writeJSON(w, mapping.HTTP, ErrorResponse{Error: ErrorBody{Code: mapping.Code, Message: mapping.Message(err)}})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

type bufferedWriter struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (b *bufferedWriter) Header() http.Header { return b.header }

func (b *bufferedWriter) WriteHeader(status int) {
	if b.status == 0 {
		b.status = status
	}
}

func (b *bufferedWriter) Write(p []byte) (int, error) {
	b.WriteHeader(http.StatusOK)
	return b.body.Write(p)
}

func (b *bufferedWriter) flushTo(w http.ResponseWriter) {
	maps.Copy(w.Header(), b.header)
	w.WriteHeader(cmp.Or(b.status, http.StatusOK))
	_, _ = w.Write(b.body.Bytes())
}
