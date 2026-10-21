package httpapi

import (
	"time"

	"sis2_highload/internal/domain"
)

type eventDTO struct {
	EventID    string            `json:"event_id"`
	Type       string            `json:"type"`
	UserID     int64             `json:"user_id"`
	Payload    map[string]string `json:"payload,omitempty"`
	OccurredAt time.Time         `json:"occurred_at,omitzero"`
}

type batchRequest struct {
	Events []eventDTO `json:"events"`
}

type deliveryDTO struct {
	Channel  string `json:"channel"`
	Status   string `json:"status"`
	Attempts int32  `json:"attempts"`
}

type resultDTO struct {
	EventID    string        `json:"event_id"`
	Deliveries []deliveryDTO `json:"deliveries"`
}

type batchResponse struct {
	Results []resultDTO `json:"results"`
}

type errorResponse struct {
	Error errorDTO `json:"error"`
}

type errorDTO struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e eventDTO) toDomain() domain.Event {
	return domain.Event{
		ID:         e.EventID,
		Type:       e.Type,
		UserID:     e.UserID,
		Payload:    e.Payload,
		OccurredAt: e.OccurredAt,
	}
}

func resultFromDomain(r domain.Result) resultDTO {
	ds := make([]deliveryDTO, len(r.Deliveries))
	for i, d := range r.Deliveries {
		ds[i] = deliveryDTO{Channel: d.Channel.String(), Status: d.Status.String(), Attempts: d.Attempts}
	}
	return resultDTO{EventID: r.EventID, Deliveries: ds}
}
