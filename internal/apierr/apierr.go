// Package apierr classifies errors once so every transport maps them the same way.
package apierr

import (
	"context"
	"errors"
	"net/http"

	"google.golang.org/grpc/codes"

	"sis2_highload/internal/domain"
)

// StatusClientClosedRequest is nginx's non-standard status for a caller that went away.
const StatusClientClosedRequest = 499

type Mapping struct {
	GRPC codes.Code
	HTTP int
	Code string
	// Log marks errors whose real cause is server-side and must be logged, not shown.
	Log    bool
	public string
}

// Message returns text safe to send to callers.
func (m Mapping) Message(err error) string {
	if m.public != "" {
		return m.public
	}
	return err.Error()
}

var (
	invalidArgument  = Mapping{GRPC: codes.InvalidArgument, HTTP: http.StatusBadRequest, Code: "INVALID_ARGUMENT"}
	notFound         = Mapping{GRPC: codes.NotFound, HTTP: http.StatusNotFound, Code: "NOT_FOUND"}
	overloaded       = Mapping{GRPC: codes.ResourceExhausted, HTTP: http.StatusTooManyRequests, Code: "RESOURCE_EXHAUSTED", public: "server overloaded, retry later"}
	unavailable      = Mapping{GRPC: codes.Unavailable, HTTP: http.StatusServiceUnavailable, Code: "UNAVAILABLE", Log: true, public: "service temporarily unavailable"}
	deadlineExceeded = Mapping{GRPC: codes.DeadlineExceeded, HTTP: http.StatusGatewayTimeout, Code: "DEADLINE_EXCEEDED", public: "deadline exceeded"}
	canceled         = Mapping{GRPC: codes.Canceled, HTTP: StatusClientClosedRequest, Code: "CANCELED", public: "request canceled"}
	internal         = Mapping{GRPC: codes.Internal, HTTP: http.StatusInternalServerError, Code: "INTERNAL", Log: true, public: "internal error"}
)

// Context errors are checked first: the service wraps a store's ctx error in
// ErrUnavailable, but the caller's deadline is the more accurate answer.
var table = []struct {
	target  error
	mapping Mapping
}{
	{context.DeadlineExceeded, deadlineExceeded},
	{context.Canceled, canceled},
	{domain.ErrInvalidArgument, invalidArgument},
	{domain.ErrNotFound, notFound},
	{domain.ErrOverloaded, overloaded},
	{domain.ErrUnavailable, unavailable},
}

// ErrInternal is what clients get back for INTERNAL or any status outside the table.
var ErrInternal = errors.New("internal server error")

// FromGRPC inverts the table for clients: it returns the sentinel a gRPC code stands for.
func FromGRPC(c codes.Code) error {
	for _, row := range table {
		if row.mapping.GRPC == c {
			return row.target
		}
	}
	return ErrInternal
}

// FromHTTP inverts the table for clients: it returns the sentinel an HTTP error status stands for.
func FromHTTP(status int) error {
	for _, row := range table {
		if row.mapping.HTTP == status {
			return row.target
		}
	}
	return ErrInternal
}

// Of classifies err; anything unrecognised is INTERNAL.
func Of(err error) Mapping {
	for _, row := range table {
		if errors.Is(err, row.target) {
			return row.mapping
		}
	}
	return internal
}
