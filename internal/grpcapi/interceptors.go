package grpcapi

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"google.golang.org/grpc"

	pb "sis2_highload/gen/notifications/v1"
	"sis2_highload/internal/faults"
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

// faultInterceptor injects faults into NotificationService RPCs only, never health checks.
// Injected errors are not logged: they are expected and would flood the log under load.
func faultInterceptor(inj *faults.Injector) grpc.UnaryServerInterceptor {
	prefix := "/" + pb.NotificationService_ServiceDesc.ServiceName + "/"
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		if inj == nil || !strings.HasPrefix(info.FullMethod, prefix) {
			return next(ctx, req)
		}
		if err := inj.Before(ctx); err != nil {
			return nil, toStatus(err)
		}
		resp, err := next(ctx, req)
		if err == nil && inj.LoseResponse() {
			return nil, toStatus(faults.ErrLostResponse)
		}
		return resp, err
	}
}
