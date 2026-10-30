package apierr

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"

	"sis2_highload/internal/domain"
)

func TestOf(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		grpc   codes.Code
		http   int
		expose bool
	}{
		{"invalid argument", fmt.Errorf("%w: bad", domain.ErrInvalidArgument), codes.InvalidArgument, 400, true},
		{"not found", fmt.Errorf("%w: x", domain.ErrNotFound), codes.NotFound, 404, true},
		{"overloaded", domain.ErrOverloaded, codes.ResourceExhausted, 429, false},
		{"unavailable", fmt.Errorf("%w: store: %w", domain.ErrUnavailable, errors.New("dial tcp")), codes.Unavailable, 503, false},
		{"deadline", context.DeadlineExceeded, codes.DeadlineExceeded, 504, false},
		{"deadline wrapped in unavailable", fmt.Errorf("%w: store: %w", domain.ErrUnavailable, context.DeadlineExceeded), codes.DeadlineExceeded, 504, false},
		{"canceled", context.Canceled, codes.Canceled, 499, false},
		{"other", errors.New("boom"), codes.Internal, 500, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mapping := Of(tt.err)
			if mapping.GRPC != tt.grpc || mapping.HTTP != tt.http {
				t.Fatalf("Of(%v) = %v/%d, want %v/%d", tt.err, mapping.GRPC, mapping.HTTP, tt.grpc, tt.http)
			}
			if exposed := mapping.Message(tt.err) == tt.err.Error(); exposed != tt.expose {
				t.Fatalf("message %q exposed=%v, want %v", mapping.Message(tt.err), exposed, tt.expose)
			}
		})
	}
}

func TestInverse(t *testing.T) {
	for _, row := range table {
		if got := FromGRPC(row.mapping.GRPC); got != row.target {
			t.Errorf("FromGRPC(%v) = %v, want %v", row.mapping.GRPC, got, row.target)
		}
		if got := FromHTTP(row.mapping.HTTP); got != row.target {
			t.Errorf("FromHTTP(%d) = %v, want %v", row.mapping.HTTP, got, row.target)
		}
		if back := Of(row.target); back != row.mapping {
			t.Errorf("Of(FromX(%v)) does not round-trip", row.target)
		}
	}
	if FromGRPC(codes.Internal) != ErrInternal || FromHTTP(500) != ErrInternal || FromHTTP(502) != ErrInternal {
		t.Error("internal and unknown statuses should map to ErrInternal")
	}
}
