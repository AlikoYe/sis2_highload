package httpapi

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestWithTimeout(t *testing.T) {
	tests := []struct {
		name       string
		header     string
		want       time.Duration
		wantStatus int
	}{
		{"absent uses default", "", DefaultTimeout, http.StatusOK},
		{"explicit", "50", 50 * time.Millisecond, http.StatusOK},
		{"capped", "60000", MaxTimeout, http.StatusOK},
		{"not a number", "soon", 0, http.StatusBadRequest},
		{"zero", "0", 0, http.StatusBadRequest},
	}
	h := &handler{log: slog.New(slog.DiscardHandler)}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got time.Duration
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				d, _ := r.Context().Deadline()
				got = time.Until(d)
			})
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.header != "" {
				req.Header.Set(TimeoutHeader, tt.header)
			}
			rec := httptest.NewRecorder()
			h.withTimeout(next).ServeHTTP(rec, req)
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
			if tt.wantStatus == http.StatusOK && (got > tt.want || got < tt.want-100*time.Millisecond) {
				t.Fatalf("remaining = %v, want ~%v", got, tt.want)
			}
		})
	}
}
