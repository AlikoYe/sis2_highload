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

// deadlineInterceptor applies defaultTimeout when the caller sent no deadline and
// caps longer deadlines at maxTimeout.
func deadlineInterceptor(defaultTimeout, maxTimeout time.Duration) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		timeout := defaultTimeout
		if deadline, ok := ctx.Deadline(); ok {
			if time.Until(deadline) <= maxTimeout {
				return next(ctx, req)
			}
			timeout = maxTimeout
		}
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return next(ctx, req)
	}
}

func recoveryInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				err = statusError(ctx, log, fmt.Errorf("panic in %s: %v", info.FullMethod, recovered))
			}
		}()
		return next(ctx, req)
	}
}

// faultInterceptor injects faults into NotificationService RPCs only, never health checks.
// Injected errors are not logged: they are expected and would flood the log under load.
func faultInterceptor(injector *faults.Injector) grpc.UnaryServerInterceptor {
	servicePrefix := "/" + pb.NotificationService_ServiceDesc.ServiceName + "/"
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		if injector == nil || !strings.HasPrefix(info.FullMethod, servicePrefix) {
			return next(ctx, req)
		}
		if err := injector.Before(ctx); err != nil {
			return nil, toStatus(err)
		}
		resp, err := next(ctx, req)
		if err == nil && injector.LoseResponse() {
			return nil, toStatus(faults.ErrLostResponse)
		}
		return resp, err
	}
}
