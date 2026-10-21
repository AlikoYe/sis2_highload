// Package workload builds the deterministic messages every measurement tool uses,
// so payload sizes, codec benchmarks and load tests all send identical data.
package workload

import (
	"fmt"
	"strings"
	"time"

	pb "sis2_highload/gen/notifications/v1"
	"sis2_highload/internal/domain"
	"sis2_highload/internal/grpcapi"
	"sis2_highload/internal/httpapi"
	"sis2_highload/internal/service"
)

type Kind string

const (
	Small  Kind = "small"
	Medium Kind = "medium"
	Large  Kind = "large"
	Batch  Kind = "batch"
)

var Kinds = []Kind{Small, Medium, Large, Batch}

const (
	BatchSize         = 100
	MediumPayloadSize = 500
	LargePayloadSize  = 3584 // 3.5 KiB, under the 4 KiB validation limit
)

var occurredAt = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

// Event builds a single-event message of kind small, medium or large.
func Event(k Kind, id string) domain.Event {
	switch k {
	case Small:
		return domain.Event{ID: id, Type: "auth.login_code", UserID: 1000042, Payload: map[string]string{"code": "483920"}, OccurredAt: occurredAt}
	case Medium:
		return domain.Event{ID: id, Type: "order.shipped", UserID: 1000042, Payload: orderPayload(MediumPayloadSize), OccurredAt: occurredAt}
	case Large:
		return domain.Event{ID: id, Type: "order.shipped", UserID: 1000042, Payload: orderPayload(LargePayloadSize), OccurredAt: occurredAt}
	}
	panic(fmt.Sprintf("workload: no single event for kind %q", k))
}

// BatchEvents builds one small event per id.
func BatchEvents(ids []string) []domain.Event {
	es := make([]domain.Event, len(ids))
	for i, id := range ids {
		es[i] = Event(Small, id)
	}
	return es
}

func ParseKind(s string) (Kind, error) {
	for _, k := range Kinds {
		if string(k) == s {
			return k, nil
		}
	}
	return "", fmt.Errorf("unknown payload kind %q", s)
}

// orderPayload is 8 realistic fields whose key+value bytes total exactly size.
func orderPayload(size int) map[string]string {
	p := map[string]string{
		"order_id":           "ORD-2026-0001842",
		"customer_name":      "Aigerim Sadykova",
		"carrier":            "Kazpost Express",
		"tracking_number":    "KZ7781234590KZ",
		"shipping_address":   "Abay Ave 150, apt 12, Almaty 050000, Kazakhstan",
		"estimated_delivery": "2026-01-04",
		"support_url":        "https://shop.example.kz/help/orders/ORD-2026-0001842",
	}
	used := len("items")
	for k, v := range p {
		used += len(k) + len(v)
	}
	const item = "1x Wireless earbuds (black); 2x USB-C cable 1m; 1x Phone case, clear; "
	p["items"] = strings.Repeat(item, (size-used)/len(item)+1)[:size-used]
	return p
}

// Result is the response the service returns for a freshly accepted event.
func Result(e domain.Event) domain.Result {
	channels, _ := service.Channels(e.Type)
	ds := make([]domain.Delivery, len(channels))
	for i, ch := range channels {
		ds[i] = domain.Delivery{EventID: e.ID, Channel: ch, Status: domain.StatusQueued}
	}
	return domain.NewResult(e.ID, ds)
}

func Results(es []domain.Event) []domain.Result {
	rs := make([]domain.Result, len(es))
	for i, e := range es {
		rs[i] = Result(e)
	}
	return rs
}

func ProtoRequest(e domain.Event) *pb.PublishRequest {
	return &pb.PublishRequest{Event: grpcapi.EventToProto(e)}
}

func ProtoBatchRequest(es []domain.Event) *pb.PublishBatchRequest {
	req := &pb.PublishBatchRequest{Events: make([]*pb.Event, len(es))}
	for i, e := range es {
		req.Events[i] = grpcapi.EventToProto(e)
	}
	return req
}

func ProtoResponse(r domain.Result) *pb.PublishResponse { return grpcapi.PublishResponse(r) }

func ProtoBatchResponse(rs []domain.Result) *pb.PublishBatchResponse {
	resp := &pb.PublishBatchResponse{Results: make([]*pb.PublishResponse, len(rs))}
	for i, r := range rs {
		resp.Results[i] = grpcapi.PublishResponse(r)
	}
	return resp
}

func JSONRequest(e domain.Event) httpapi.Event { return httpapi.EventFromDomain(e) }

func JSONBatchRequest(es []domain.Event) httpapi.BatchRequest {
	req := httpapi.BatchRequest{Events: make([]httpapi.Event, len(es))}
	for i, e := range es {
		req.Events[i] = httpapi.EventFromDomain(e)
	}
	return req
}

func JSONResponse(r domain.Result) httpapi.Result { return httpapi.ResultFromDomain(r) }

func JSONBatchResponse(rs []domain.Result) httpapi.BatchResponse {
	resp := httpapi.BatchResponse{Results: make([]httpapi.Result, len(rs))}
	for i, r := range rs {
		resp.Results[i] = httpapi.ResultFromDomain(r)
	}
	return resp
}
