package grpcapi

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	pb "sis2_highload/gen/notifications/v1"
	"sis2_highload/internal/apierr"
	"sis2_highload/internal/domain"
	"sis2_highload/internal/faults"
	"sis2_highload/internal/service"
)

const (
	DefaultTimeout = 2 * time.Second
	MaxTimeout     = 5 * time.Second

	keepaliveMaxIdle      = 5 * time.Minute
	keepalivePingInterval = time.Minute
	keepalivePingTimeout  = 20 * time.Second
	keepaliveMinClientGap = 10 * time.Second // reject clients pinging more often than this
)

type handler struct {
	pb.UnimplementedNotificationServiceServer
	svc *service.Service
	log *slog.Logger
}

// New builds a gRPC server with the notification and health services registered.
// Health starts NOT_SERVING; call SetServing once listeners are up. injector may be nil.
func New(svc *service.Service, log *slog.Logger, injector *faults.Injector) (*grpc.Server, *health.Server) {
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			recoveryInterceptor(log),
			deadlineInterceptor(DefaultTimeout, MaxTimeout),
			faultInterceptor(injector),
		),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: keepaliveMaxIdle,
			Time:              keepalivePingInterval,
			Timeout:           keepalivePingTimeout,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             keepaliveMinClientGap,
			PermitWithoutStream: true,
		}),
	)
	pb.RegisterNotificationServiceServer(srv, &handler{svc: svc, log: log})

	healthSrv := health.NewServer()
	SetServing(healthSrv, false)
	healthpb.RegisterHealthServer(srv, healthSrv)
	return srv, healthSrv
}

func SetServing(healthSrv *health.Server, serving bool) {
	servingStatus := healthpb.HealthCheckResponse_NOT_SERVING
	if serving {
		servingStatus = healthpb.HealthCheckResponse_SERVING
	}
	healthSrv.SetServingStatus("", servingStatus)
	healthSrv.SetServingStatus(pb.NotificationService_ServiceDesc.ServiceName, servingStatus)
}

func (h *handler) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	if req.GetEvent() == nil {
		return nil, statusError(ctx, h.log, fmt.Errorf("%w: event is required", domain.ErrInvalidArgument))
	}
	result, err := h.svc.Publish(ctx, EventFromProto(req.GetEvent()))
	if err != nil {
		return nil, statusError(ctx, h.log, err)
	}
	return PublishResponse(result), nil
}

func (h *handler) PublishBatch(ctx context.Context, req *pb.PublishBatchRequest) (*pb.PublishBatchResponse, error) {
	events := make([]domain.Event, len(req.GetEvents()))
	for i, event := range req.GetEvents() {
		if event == nil {
			return nil, statusError(ctx, h.log, fmt.Errorf("events[%d]: %w: event is required", i, domain.ErrInvalidArgument))
		}
		events[i] = EventFromProto(event)
	}
	results, err := h.svc.PublishBatch(ctx, events)
	if err != nil {
		return nil, statusError(ctx, h.log, err)
	}
	return PublishBatchResponse(results), nil
}

func (h *handler) GetStatus(ctx context.Context, req *pb.GetStatusRequest) (*pb.GetStatusResponse, error) {
	result, err := h.svc.GetStatus(ctx, req.GetEventId())
	if err != nil {
		return nil, statusError(ctx, h.log, err)
	}
	return &pb.GetStatusResponse{EventId: result.EventID, Deliveries: DeliveriesToProto(result.Deliveries)}, nil
}

func statusError(ctx context.Context, log *slog.Logger, err error) error {
	if mapping := apierr.Of(err); mapping.Log {
		log.ErrorContext(ctx, "grpc request failed", "code", mapping.Code, "err", err)
	}
	return toStatus(err)
}

func toStatus(err error) error {
	mapping := apierr.Of(err)
	return status.Error(mapping.GRPC, mapping.Message(err))
}
