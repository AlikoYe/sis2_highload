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

const retryAfterTrailer = "retry-after-ms"

type grpcClient struct {
	caller
	conn *grpc.ClientConn
	api  pb.NotificationServiceClient
}

// NewGRPC connects lazily to target; extra dial options are appended (e.g. a test dialer).
func NewGRPC(target string, opts Options, dialOpts ...grpc.DialOption) (Client, error) {
	all := append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	}, dialOpts...)
	conn, err := grpc.NewClient(target, all...)
	if err != nil {
		return nil, fmt.Errorf("grpc client %s: %w", target, err)
	}
	return &grpcClient{caller: newCaller(opts), conn: conn, api: pb.NewNotificationServiceClient(conn)}, nil
}

func (c *grpcClient) Publish(ctx context.Context, e domain.Event) (domain.Result, error) {
	req := &pb.PublishRequest{Event: grpcapi.EventToProto(e)}
	var res domain.Result
	err := c.call(ctx, func(ctx context.Context) error {
		var trailer metadata.MD
		resp, err := c.api.Publish(ctx, req, grpc.Trailer(&trailer))
		if err != nil {
			return grpcError(err, trailer)
		}
		res = grpcapi.ResultFromProto(resp.GetEventId(), resp.GetDeliveries())
		return nil
	})
	return res, err
}

func (c *grpcClient) PublishBatch(ctx context.Context, es []domain.Event) ([]domain.Result, error) {
	req := &pb.PublishBatchRequest{Events: make([]*pb.Event, len(es))}
	for i, e := range es {
		req.Events[i] = grpcapi.EventToProto(e)
	}
	var res []domain.Result
	err := c.call(ctx, func(ctx context.Context) error {
		var trailer metadata.MD
		resp, err := c.api.PublishBatch(ctx, req, grpc.Trailer(&trailer))
		if err != nil {
			return grpcError(err, trailer)
		}
		res = make([]domain.Result, len(resp.GetResults()))
		for i, r := range resp.GetResults() {
			res[i] = grpcapi.ResultFromProto(r.GetEventId(), r.GetDeliveries())
		}
		return nil
	})
	return res, err
}

func (c *grpcClient) GetStatus(ctx context.Context, eventID string) (domain.Result, error) {
	req := &pb.GetStatusRequest{EventId: eventID}
	var res domain.Result
	err := c.call(ctx, func(ctx context.Context) error {
		var trailer metadata.MD
		resp, err := c.api.GetStatus(ctx, req, grpc.Trailer(&trailer))
		if err != nil {
			return grpcError(err, trailer)
		}
		res = grpcapi.ResultFromProto(resp.GetEventId(), resp.GetDeliveries())
		return nil
	})
	return res, err
}

func (c *grpcClient) Close() error { return c.conn.Close() }

// grpcError maps a status back to a domain error. A refused or reset connection
// arrives as codes.Unavailable, so it is retryable like a server-side 503.
func grpcError(err error, trailer metadata.MD) error {
	st := status.Convert(err)
	re := &RemoteError{Kind: apierr.FromGRPC(st.Code()), Message: st.Message()}
	if v := trailer.Get(retryAfterTrailer); len(v) > 0 {
		if ms, perr := strconv.Atoi(v[0]); perr == nil && ms > 0 {
			re.RetryAfter = time.Duration(ms) * time.Millisecond
		}
	}
	return re
}
