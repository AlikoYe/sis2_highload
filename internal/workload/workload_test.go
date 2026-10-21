package workload

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/google/uuid"

	"sis2_highload/internal/service"
	"sis2_highload/internal/store/memory"
)

func payloadBytes(p map[string]string) int {
	n := 0
	for k, v := range p {
		n += len(k) + len(v)
	}
	return n
}

func TestEventsAreValidAndSized(t *testing.T) {
	svc := service.New(memory.New())
	tests := []struct {
		kind      Kind
		wantBytes int
		wantKeys  int
	}{
		{Small, len("code") + len("483920"), 1},
		{Medium, MediumPayloadSize, 8},
		{Large, LargePayloadSize, 8},
	}
	for _, tt := range tests {
		t.Run(string(tt.kind), func(t *testing.T) {
			e := Event(tt.kind, uuid.NewString())
			if got := payloadBytes(e.Payload); got != tt.wantBytes || len(e.Payload) != tt.wantKeys {
				t.Fatalf("payload = %d bytes / %d keys, want %d / %d", got, len(e.Payload), tt.wantBytes, tt.wantKeys)
			}
			res, err := svc.Publish(context.Background(), e)
			if err != nil {
				t.Fatalf("service rejected workload event: %v", err)
			}
			if want := Result(e); !reflect.DeepEqual(res, want) {
				t.Fatalf("Result() = %+v, service returned %+v", want, res)
			}
		})
	}
}

func TestDeterministic(t *testing.T) {
	if !reflect.DeepEqual(Event(Large, "x"), Event(Large, "x")) {
		t.Fatal("same id must build identical events")
	}
}

func TestBatchAccepted(t *testing.T) {
	ids := make([]string, BatchSize)
	for i := range ids {
		ids[i] = fmt.Sprint(uuid.New())
	}
	es := BatchEvents(ids)
	res, err := service.New(memory.New()).PublishBatch(context.Background(), es)
	if err != nil {
		t.Fatalf("batch rejected: %v", err)
	}
	if !reflect.DeepEqual(res, Results(es)) {
		t.Fatal("Results() does not match service output")
	}
}
