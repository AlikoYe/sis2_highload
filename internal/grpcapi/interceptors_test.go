package grpcapi

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDeadlineInterceptor(t *testing.T) {
	tests := []struct {
		name   string
		client time.Duration // 0 = no deadline
		want   time.Duration
	}{
		{"no deadline gets default", 0, DefaultTimeout},
		{"short deadline kept", time.Second, time.Second},
		{"long deadline capped", time.Minute, MaxTimeout},
	}
	intercept := deadlineInterceptor(DefaultTimeout, MaxTimeout)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.client > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tt.client)
				defer cancel()
			}
			var got time.Duration
			_, _ = intercept(ctx, nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
				d, _ := ctx.Deadline()
				got = time.Until(d)
				return nil, nil
			})
			if got > tt.want || got < tt.want-100*time.Millisecond {
				t.Fatalf("remaining = %v, want ~%v", got, tt.want)
			}
		})
	}
}

func TestRecoveryInterceptor(t *testing.T) {
	intercept := recoveryInterceptor(slog.New(slog.DiscardHandler))
	_, err := intercept(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/x"}, func(context.Context, any) (any, error) {
		panic("boom")
	})
	if status.Code(err) != codes.Internal || status.Convert(err).Message() != "internal error" {
		t.Fatalf("err = %v, want Internal with generic message", err)
	}
}
