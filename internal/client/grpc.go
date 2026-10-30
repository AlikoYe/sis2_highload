package client

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "sis2_highload/gen/notifications/v1"
	"sis2_highload/internal/apierr"
	"sis2_highload/internal/domain"
	"sis2_highload/internal/grpcapi"
)

const (
	retryAfterTrailer = "retry-after-ms"

	keepalivePingInterval = 30 * time.Second
	keepalivePingTimeout  = 10 * time.Second
)

type grpcClient struct {
	caller
	conn *grpc.ClientConn
	api  pb.NotificationServiceClient
}

// NewGRPC connects lazily to target; extra dial options are appended (e.g. a test dialer).
func NewGRPC(target string, opts Options, extraDialOpts ...grpc.DialOption) (Client, error) {
	dialOpts := append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                keepalivePingInterval,
			Timeout:             keepalivePingTimeout,
			PermitWithoutStream: true,
		}),
	}, extraDialOpts...)
	conn, err := grpc.NewClient(target, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("grpc client %s: %w", target, err)
	}
	return &grpcClient{caller: newCaller(opts), conn: conn, api: pb.NewNotificationServiceClient(conn)}, nil
}

func (c *grpcClient) Publish(ctx context.Context, e domain.Event) (domain.Result, error) {
	req := &pb.PublishRequest{Event: grpcapi.EventToProto(e)}
	resp, err := callRPC(ctx, &c.caller, func(ctx context.Context, opts ...grpc.CallOption) (*pb.PublishResponse, error) {
		return c.api.Publish(ctx, req, opts...)
	})
	if err != nil {
		return domain.Result{}, err
	}
	return grpcapi.ResultFromProto(resp.GetEventId(), resp.GetDeliveries()), nil
}

func (c *grpcClient) PublishBatch(ctx context.Context, es []domain.Event) ([]domain.Result, error) {
	req := &pb.PublishBatchRequest{Events: grpcapi.EventsToProto(es)}
	resp, err := callRPC(ctx, &c.caller, func(ctx context.Context, opts ...grpc.CallOption) (*pb.PublishBatchResponse, error) {
		return c.api.PublishBatch(ctx, req, opts...)
	})
	if err != nil {
		return nil, err
	}
	return grpcapi.ResultsFromProto(resp), nil
}

func (c *grpcClient) GetStatus(ctx context.Context, eventID string) (domain.Result, error) {
	req := &pb.GetStatusRequest{EventId: eventID}
	resp, err := callRPC(ctx, &c.caller, func(ctx context.Context, opts ...grpc.CallOption) (*pb.GetStatusResponse, error) {
		return c.api.GetStatus(ctx, req, opts...)
	})
	if err != nil {
		return domain.Result{}, err
	}
	return grpcapi.ResultFromProto(resp.GetEventId(), resp.GetDeliveries()), nil
}

func (c *grpcClient) Close() error { return c.conn.Close() }

// callRPC runs one unary RPC through the shared retry loop, capturing the
// trailer so a server's retry-after hint reaches the retry policy.
func callRPC[Resp any](ctx context.Context, c *caller, rpc func(context.Context, ...grpc.CallOption) (Resp, error)) (Resp, error) {
	var resp Resp
	err := c.call(ctx, func(ctx context.Context) error {
		var trailer metadata.MD
		attemptResp, err := rpc(ctx, grpc.Trailer(&trailer))
		if err != nil {
			return grpcError(err, trailer)
		}
		resp = attemptResp
		return nil
	})
	return resp, err
}

// grpcError maps a status back to a domain error. A refused or reset connection
// arrives as codes.Unavailable, so it is retryable like a server-side 503.
func grpcError(err error, trailer metadata.MD) error {
	st := status.Convert(err)
	remoteErr := &RemoteError{Kind: apierr.FromGRPC(st.Code()), Message: st.Message()}
	if values := trailer.Get(retryAfterTrailer); len(values) > 0 {
		if ms, parseErr := strconv.Atoi(values[0]); parseErr == nil && ms > 0 {
			remoteErr.RetryAfter = time.Duration(ms) * time.Millisecond
		}
	}
	return remoteErr
}
