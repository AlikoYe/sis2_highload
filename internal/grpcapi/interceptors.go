package grpcapi

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc"
)

// deadlineInterceptor applies def when the caller sent no deadline and caps longer ones at max.
func deadlineInterceptor(def, max time.Duration) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		d, ok := ctx.Deadline()
		switch {
		case !ok:
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, def)
			defer cancel()
		case time.Until(d) > max:
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, max)
			defer cancel()
		}
		return next(ctx, req)
	}
}

func recoveryInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if v := recover(); v != nil {
				err = statusError(ctx, log, fmt.Errorf("panic in %s: %v", info.FullMethod, v))
			}
		}()
		return next(ctx, req)
	}
}
