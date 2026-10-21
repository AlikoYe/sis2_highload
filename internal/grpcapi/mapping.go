package grpcapi

import (
	"time"

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

func eventFromProto(e *pb.Event) domain.Event {
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

func deliveriesToProto(ds []domain.Delivery) []*pb.Delivery {
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

func publishResponse(r domain.Result) *pb.PublishResponse {
	return &pb.PublishResponse{EventId: r.EventID, Deliveries: deliveriesToProto(r.Deliveries)}
}
