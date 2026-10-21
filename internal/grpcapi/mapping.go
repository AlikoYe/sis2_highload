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

func invert[K, V comparable](m map[K]V) map[V]K {
	out := make(map[V]K, len(m))
	for k, v := range m {
		out[v] = k
	}
	return out
}

func EventFromProto(e *pb.Event) domain.Event {
	var occurredAt time.Time
	if e.GetOccurredAt() != nil {
		occurredAt = e.GetOccurredAt().AsTime()
	}
	return domain.Event{
		ID:         e.GetEventId(),
		Type:       e.GetType(),
		UserID:     e.GetUserId(),
		Payload:    e.GetPayload(),
		OccurredAt: occurredAt,
	}
}

func EventToProto(e domain.Event) *pb.Event {
	out := &pb.Event{EventId: e.ID, Type: e.Type, UserId: e.UserID, Payload: e.Payload}
	if !e.OccurredAt.IsZero() {
		out.OccurredAt = timestamppb.New(e.OccurredAt)
	}
	return out
}

func DeliveriesToProto(ds []domain.Delivery) []*pb.Delivery {
	out := make([]*pb.Delivery, len(ds))
	for i, d := range ds {
		out[i] = &pb.Delivery{
			Channel:  channelToProto[d.Channel],
			Status:   statusToProto[d.Status],
			Attempts: d.Attempts,
		}
	}
	return out
}

func ResultFromProto(eventID string, ds []*pb.Delivery) domain.Result {
	out := make([]domain.Delivery, len(ds))
	for i, d := range ds {
		out[i] = domain.Delivery{
			EventID:  eventID,
			Channel:  channelFromProto[d.GetChannel()],
			Status:   statusFromProto[d.GetStatus()],
			Attempts: d.GetAttempts(),
		}
	}
	return domain.NewResult(eventID, out)
}

func PublishResponse(r domain.Result) *pb.PublishResponse {
	return &pb.PublishResponse{EventId: r.EventID, Deliveries: DeliveriesToProto(r.Deliveries)}
}
