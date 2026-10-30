package grpcapi

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	pb "sis2_highload/gen/notifications/v1"
	"sis2_highload/internal/domain"
)

var channelToProto = map[domain.Channel]pb.Channel{
	domain.ChannelPush:  pb.Channel_CHANNEL_PUSH,
	domain.ChannelEmail: pb.Channel_CHANNEL_EMAIL,
}

var statusToProto = map[domain.Status]pb.DeliveryStatus{
	domain.StatusQueued:  pb.DeliveryStatus_DELIVERY_STATUS_QUEUED,
	domain.StatusSending: pb.DeliveryStatus_DELIVERY_STATUS_SENDING,
	domain.StatusRetry:   pb.DeliveryStatus_DELIVERY_STATUS_RETRY,
	domain.StatusSent:    pb.DeliveryStatus_DELIVERY_STATUS_SENT,
	domain.StatusFailed:  pb.DeliveryStatus_DELIVERY_STATUS_FAILED,
}

var (
	channelFromProto = invert(channelToProto)
	statusFromProto  = invert(statusToProto)
)

func invert[K, V comparable](forward map[K]V) map[V]K {
	inverted := make(map[V]K, len(forward))
	for k, v := range forward {
		inverted[v] = k
	}
	return inverted
}

func EventFromProto(event *pb.Event) domain.Event {
	var occurredAt time.Time
	if event.GetOccurredAt() != nil {
		occurredAt = event.GetOccurredAt().AsTime()
	}
	return domain.Event{
		ID:         event.GetEventId(),
		Type:       event.GetType(),
		UserID:     event.GetUserId(),
		Payload:    event.GetPayload(),
		OccurredAt: occurredAt,
	}
}

func EventToProto(event domain.Event) *pb.Event {
	out := &pb.Event{EventId: event.ID, Type: event.Type, UserId: event.UserID, Payload: event.Payload}
	if !event.OccurredAt.IsZero() {
		out.OccurredAt = timestamppb.New(event.OccurredAt)
	}
	return out
}

func EventsToProto(events []domain.Event) []*pb.Event {
	out := make([]*pb.Event, len(events))
	for i, event := range events {
		out[i] = EventToProto(event)
	}
	return out
}

func DeliveriesToProto(deliveries []domain.Delivery) []*pb.Delivery {
	out := make([]*pb.Delivery, len(deliveries))
	for i, d := range deliveries {
		out[i] = &pb.Delivery{
			Channel:  channelToProto[d.Channel],
			Status:   statusToProto[d.Status],
			Attempts: d.Attempts,
		}
	}
	return out
}

func ResultFromProto(eventID string, deliveries []*pb.Delivery) domain.Result {
	out := make([]domain.Delivery, len(deliveries))
	for i, d := range deliveries {
		out[i] = domain.Delivery{
			EventID:  eventID,
			Channel:  channelFromProto[d.GetChannel()],
			Status:   statusFromProto[d.GetStatus()],
			Attempts: d.GetAttempts(),
		}
	}
	return domain.NewResult(eventID, out)
}

func ResultsFromProto(resp *pb.PublishBatchResponse) []domain.Result {
	results := make([]domain.Result, len(resp.GetResults()))
	for i, r := range resp.GetResults() {
		results[i] = ResultFromProto(r.GetEventId(), r.GetDeliveries())
	}
	return results
}

func PublishResponse(result domain.Result) *pb.PublishResponse {
	return &pb.PublishResponse{EventId: result.EventID, Deliveries: DeliveriesToProto(result.Deliveries)}
}

func PublishBatchResponse(results []domain.Result) *pb.PublishBatchResponse {
	out := make([]*pb.PublishResponse, len(results))
	for i, result := range results {
		out[i] = PublishResponse(result)
	}
	return &pb.PublishBatchResponse{Results: out}
}
