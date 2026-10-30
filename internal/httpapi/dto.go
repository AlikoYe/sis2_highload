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

func BatchRequestFromDomain(events []domain.Event) BatchRequest {
	req := BatchRequest{Events: make([]Event, len(events))}
	for i, event := range events {
		req.Events[i] = EventFromDomain(event)
	}
	return req
}

func (req BatchRequest) ToDomain() []domain.Event {
	events := make([]domain.Event, len(req.Events))
	for i, event := range req.Events {
		events[i] = event.ToDomain()
	}
	return events
}

func ResultFromDomain(r domain.Result) Result {
	deliveries := make([]Delivery, len(r.Deliveries))
	for i, d := range r.Deliveries {
		deliveries[i] = Delivery{Channel: d.Channel.String(), Status: d.Status.String(), Attempts: d.Attempts}
	}
	return Result{EventID: r.EventID, Deliveries: deliveries}
}

func (r Result) ToDomain() domain.Result {
	deliveries := make([]domain.Delivery, len(r.Deliveries))
	for i, d := range r.Deliveries {
		deliveries[i] = domain.Delivery{
			EventID:  r.EventID,
			Channel:  domain.Channel(d.Channel),
			Status:   domain.Status(d.Status),
			Attempts: d.Attempts,
		}
	}
	return domain.NewResult(r.EventID, deliveries)
}

func BatchResponseFromDomain(results []domain.Result) BatchResponse {
	resp := BatchResponse{Results: make([]Result, len(results))}
	for i, result := range results {
		resp.Results[i] = ResultFromDomain(result)
	}
	return resp
}

func (resp BatchResponse) ToDomain() []domain.Result {
	results := make([]domain.Result, len(resp.Results))
	for i, result := range resp.Results {
		results[i] = result.ToDomain()
	}
	return results
}
