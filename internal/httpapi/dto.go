package httpapi

import (
	"time"

	"sis2_highload/internal/domain"
)

// Wire types for the REST/JSON API, shared by the server and internal/client.

type Event struct {
	EventID    string            `json:"event_id"`
	Type       string            `json:"type"`
	UserID     int64             `json:"user_id"`
	Payload    map[string]string `json:"payload,omitempty"`
	OccurredAt time.Time         `json:"occurred_at,omitzero"`
}

type BatchRequest struct {
	Events []Event `json:"events"`
}

type Delivery struct {
	Channel  string `json:"channel"`
	Status   string `json:"status"`
	Attempts int32  `json:"attempts"`
}

type Result struct {
	EventID    string     `json:"event_id"`
	Deliveries []Delivery `json:"deliveries"`
}

type BatchResponse struct {
	Results []Result `json:"results"`
}

type ErrorResponse struct {
	Error ErrorBody `json:"error"`
}

type ErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func EventFromDomain(e domain.Event) Event {
	return Event{EventID: e.ID, Type: e.Type, UserID: e.UserID, Payload: e.Payload, OccurredAt: e.OccurredAt}
}

func (e Event) ToDomain() domain.Event {
	return domain.Event{ID: e.EventID, Type: e.Type, UserID: e.UserID, Payload: e.Payload, OccurredAt: e.OccurredAt}
}

func ResultFromDomain(r domain.Result) Result {
	ds := make([]Delivery, len(r.Deliveries))
	for i, d := range r.Deliveries {
		ds[i] = Delivery{Channel: d.Channel.String(), Status: d.Status.String(), Attempts: d.Attempts}
	}
	return Result{EventID: r.EventID, Deliveries: ds}
}

func (r Result) ToDomain() domain.Result {
	ds := make([]domain.Delivery, len(r.Deliveries))
	for i, d := range r.Deliveries {
		ds[i] = domain.Delivery{EventID: r.EventID, Channel: domain.Channel(d.Channel), Status: domain.Status(d.Status), Attempts: d.Attempts}
	}
	return domain.NewResult(r.EventID, ds)
}
