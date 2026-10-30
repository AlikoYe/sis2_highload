package domain

import (
	"slices"
	"strings"
	"time"
)

type Channel string

const (
	ChannelPush  Channel = "PUSH"
	ChannelEmail Channel = "EMAIL"
)

func (c Channel) String() string { return string(c) }

type Status string

const (
	StatusQueued  Status = "QUEUED"
	StatusSending Status = "SENDING"
	StatusRetry   Status = "RETRY"
	StatusSent    Status = "SENT"
	StatusFailed  Status = "FAILED"
)

func (s Status) String() string { return string(s) }

type Event struct {
	ID         string
	Type       string
	UserID     int64
	Payload    map[string]string
	OccurredAt time.Time
}

type Delivery struct {
	EventID  string
	Channel  Channel
	Status   Status
	Attempts int32
}

type Result struct {
	EventID    string
	Deliveries []Delivery
}

// NewResult sorts deliveries by channel so responses are deterministic.
func NewResult(eventID string, deliveries []Delivery) Result {
	SortDeliveries(deliveries)
	return Result{EventID: eventID, Deliveries: deliveries}
}

func SortDeliveries(deliveries []Delivery) {
	slices.SortFunc(deliveries, func(a, b Delivery) int {
		return strings.Compare(string(a.Channel), string(b.Channel))
	})
}
