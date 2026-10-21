package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"sis2_highload/internal/apierr"
	"sis2_highload/internal/domain"
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
	svc   *service.Service
	ready Pinger
	log   *slog.Logger
}

func New(svc *service.Service, ready Pinger, log *slog.Logger) http.Handler {
	h := &handler{svc: svc, ready: ready, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/events", h.publish)
	mux.HandleFunc("POST /v1/events/batch", h.publishBatch)
	mux.HandleFunc("GET /v1/events/{id}", h.getStatus)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", h.readyz)
	return h.recover(h.withTimeout(mux))
}

func (h *handler) publish(w http.ResponseWriter, r *http.Request) {
	var req eventDTO
	if !h.decode(w, r, &req) {
		return
	}
	res, err := h.svc.Publish(r.Context(), req.toDomain())
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusAccepted, resultFromDomain(res))
}

func (h *handler) publishBatch(w http.ResponseWriter, r *http.Request) {
	var req batchRequest
	if !h.decode(w, r, &req) {
		return
	}
	events := make([]domain.Event, len(req.Events))
	for i, e := range req.Events {
		events[i] = e.toDomain()
	}
	results, err := h.svc.PublishBatch(r.Context(), events)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	out := batchResponse{Results: make([]resultDTO, len(results))}
	for i, res := range results {
		out.Results[i] = resultFromDomain(res)
	}
	writeJSON(w, http.StatusAccepted, out)
}

func (h *handler) getStatus(w http.ResponseWriter, r *http.Request) {
	res, err := h.svc.GetStatus(r.Context(), r.PathValue("id"))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, resultFromDomain(res))
}

func (h *handler) readyz(w http.ResponseWriter, r *http.Request) {
	if err := h.ready.Ping(r.Context()); err != nil {
		h.log.WarnContext(r.Context(), "readiness check failed", "err", err)
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *handler) decode(w http.ResponseWriter, r *http.Request, v any) bool {
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
		if v := r.Header.Get(TimeoutHeader); v != "" {
			ms, err := strconv.Atoi(v)
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
			if v := recover(); v != nil {
				if v == http.ErrAbortHandler {
					panic(v)
				}
				h.writeError(w, r, fmt.Errorf("panic in %s %s: %v", r.Method, r.URL.Path, v))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (h *handler) writeError(w http.ResponseWriter, r *http.Request, err error) {
	m := apierr.Of(err)
	if m.Log {
		h.log.ErrorContext(r.Context(), "http request failed", "code", m.Code, "method", r.Method, "path", r.URL.Path, "err", err)
	}
	writeJSON(w, m.HTTP, errorResponse{Error: errorDTO{Code: m.Code, Message: m.Message(err)}})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
