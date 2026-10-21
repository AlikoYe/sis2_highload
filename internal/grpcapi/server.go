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
	"sis2_highload/internal/service"
)

const (
	DefaultTimeout = 2 * time.Second
	MaxTimeout     = 5 * time.Second
)

type handler struct {
	pb.UnimplementedNotificationServiceServer
	svc *service.Service
	log *slog.Logger
}

// New builds a gRPC server with the notification and health services registered.
// Health starts NOT_SERVING; call SetServing once listeners are up.
func New(svc *service.Service, log *slog.Logger) (*grpc.Server, *health.Server) {
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(recoveryInterceptor(log), deadlineInterceptor(DefaultTimeout, MaxTimeout)),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 5 * time.Minute,
			Time:              time.Minute,
			Timeout:           20 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	pb.RegisterNotificationServiceServer(srv, &handler{svc: svc, log: log})

	hs := health.NewServer()
	SetServing(hs, false)
	healthpb.RegisterHealthServer(srv, hs)
	return srv, hs
}

func SetServing(hs *health.Server, serving bool) {
	st := healthpb.HealthCheckResponse_NOT_SERVING
	if serving {
		st = healthpb.HealthCheckResponse_SERVING
	}
	hs.SetServingStatus("", st)
	hs.SetServingStatus(pb.NotificationService_ServiceDesc.ServiceName, st)
}

func (h *handler) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	if req.GetEvent() == nil {
		return nil, statusError(ctx, h.log, fmt.Errorf("%w: event is required", domain.ErrInvalidArgument))
	}
	res, err := h.svc.Publish(ctx, eventFromProto(req.GetEvent()))
	if err != nil {
		return nil, statusError(ctx, h.log, err)
	}
	return publishResponse(res), nil
}

func (h *handler) PublishBatch(ctx context.Context, req *pb.PublishBatchRequest) (*pb.PublishBatchResponse, error) {
	events := make([]domain.Event, len(req.GetEvents()))
	for i, e := range req.GetEvents() {
		if e == nil {
			return nil, statusError(ctx, h.log, fmt.Errorf("events[%d]: %w: event is required", i, domain.ErrInvalidArgument))
		}
		events[i] = eventFromProto(e)
	}
	results, err := h.svc.PublishBatch(ctx, events)
	if err != nil {
		return nil, statusError(ctx, h.log, err)
	}
	out := make([]*pb.PublishResponse, len(results))
	for i, r := range results {
		out[i] = publishResponse(r)
	}
	return &pb.PublishBatchResponse{Results: out}, nil
}

func (h *handler) GetStatus(ctx context.Context, req *pb.GetStatusRequest) (*pb.GetStatusResponse, error) {
	res, err := h.svc.GetStatus(ctx, req.GetEventId())
	if err != nil {
		return nil, statusError(ctx, h.log, err)
	}
	return &pb.GetStatusResponse{EventId: res.EventID, Deliveries: deliveriesToProto(res.Deliveries)}, nil
}

func statusError(ctx context.Context, log *slog.Logger, err error) error {
	m := apierr.Of(err)
	if m.Log {
		log.ErrorContext(ctx, "grpc request failed", "code", m.Code, "err", err)
	}
	return status.Error(m.GRPC, m.Message(err))
}
