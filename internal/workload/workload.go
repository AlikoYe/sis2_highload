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

	workloadUserID = 1000042
)

var occurredAt = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

// Event builds a single-event message of kind small, medium or large.
func Event(kind Kind, id string) domain.Event {
	event := domain.Event{ID: id, Type: "order.shipped", UserID: workloadUserID, OccurredAt: occurredAt}
	switch kind {
	case Small:
		event.Type = "auth.login_code"
		event.Payload = map[string]string{"code": "483920"}
	case Medium:
		event.Payload = orderPayload(MediumPayloadSize)
	case Large:
		event.Payload = orderPayload(LargePayloadSize)
	default:
		panic(fmt.Sprintf("workload: no single event for kind %q", kind))
	}
	return event
}

// BatchEvents builds one small event per id.
func BatchEvents(ids []string) []domain.Event {
	events := make([]domain.Event, len(ids))
	for i, id := range ids {
		events[i] = Event(Small, id)
	}
	return events
}

func ParseKind(name string) (Kind, error) {
	for _, kind := range Kinds {
		if string(kind) == name {
			return kind, nil
		}
	}
	return "", fmt.Errorf("unknown payload kind %q", name)
}

// orderPayload is 8 realistic fields whose key+value bytes total exactly size.
func orderPayload(size int) map[string]string {
	payload := map[string]string{
		"order_id":           "ORD-2026-0001842",
		"customer_name":      "Aigerim Sadykova",
		"carrier":            "Kazpost Express",
		"tracking_number":    "KZ7781234590KZ",
		"shipping_address":   "Abay Ave 150, apt 12, Almaty 050000, Kazakhstan",
		"estimated_delivery": "2026-01-04",
		"support_url":        "https://shop.example.kz/help/orders/ORD-2026-0001842",
	}
	used := len("items")
	for key, value := range payload {
		used += len(key) + len(value)
	}
	remaining := size - used
	const item = "1x Wireless earbuds (black); 2x USB-C cable 1m; 1x Phone case, clear; "
	payload["items"] = strings.Repeat(item, remaining/len(item)+1)[:remaining]
	return payload
}

// Result is the response the service returns for a freshly accepted event.
func Result(event domain.Event) domain.Result {
	channels, _ := service.Channels(event.Type)
	deliveries := make([]domain.Delivery, len(channels))
	for i, ch := range channels {
		deliveries[i] = domain.Delivery{EventID: event.ID, Channel: ch, Status: domain.StatusQueued}
	}
	return domain.NewResult(event.ID, deliveries)
}

func Results(events []domain.Event) []domain.Result {
	results := make([]domain.Result, len(events))
	for i, event := range events {
		results[i] = Result(event)
	}
	return results
}

func ProtoRequest(event domain.Event) *pb.PublishRequest {
	return &pb.PublishRequest{Event: grpcapi.EventToProto(event)}
}

func ProtoBatchRequest(events []domain.Event) *pb.PublishBatchRequest {
	return &pb.PublishBatchRequest{Events: grpcapi.EventsToProto(events)}
}

func ProtoResponse(result domain.Result) *pb.PublishResponse { return grpcapi.PublishResponse(result) }

func ProtoBatchResponse(results []domain.Result) *pb.PublishBatchResponse {
	return grpcapi.PublishBatchResponse(results)
}

func JSONRequest(event domain.Event) httpapi.Event { return httpapi.EventFromDomain(event) }

func JSONBatchRequest(events []domain.Event) httpapi.BatchRequest {
	return httpapi.BatchRequestFromDomain(events)
}

func JSONResponse(result domain.Result) httpapi.Result { return httpapi.ResultFromDomain(result) }

func JSONBatchResponse(results []domain.Result) httpapi.BatchResponse {
	return httpapi.BatchResponseFromDomain(results)
}
