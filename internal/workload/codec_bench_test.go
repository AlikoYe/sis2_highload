package workload

import (
	"encoding/json"
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"

	"sis2_highload/internal/domain"
	"sis2_highload/internal/httpapi"
)

const benchID = "019a0a3c-7f1e-7c4b-9d2a-5b8e1f3c4d6a"

// codecCase holds one scenario in both encodings; new*() return fresh zero values to decode into.
type codecCase struct {
	name                      string
	protoReq, protoResp       proto.Message
	jsonReq, jsonResp         any
	newProtoReq, newProtoResp func() proto.Message
	newJSONReq, newJSONResp   func() any
}

func codecCases() []codecCase {
	single := func(k Kind) codecCase {
		e := Event(k, benchID)
		r := Result(e)
		return codecCase{
			name:         string(k),
			protoReq:     ProtoRequest(e),
			protoResp:    ProtoResponse(r),
			jsonReq:      JSONRequest(e),
			jsonResp:     JSONResponse(r),
			newProtoReq:  func() proto.Message { return ProtoRequest(domain.Event{}).ProtoReflect().New().Interface() },
			newProtoResp: func() proto.Message { return ProtoResponse(domain.Result{}).ProtoReflect().New().Interface() },
			newJSONReq:   func() any { return new(httpapi.Event) },
			newJSONResp:  func() any { return new(httpapi.Result) },
		}
	}
	ids := make([]string, BatchSize)
	for i := range ids {
		ids[i] = fmt.Sprintf("019a0a3c-7f1e-7c4b-9d2a-%012d", i)
	}
	es := BatchEvents(ids)
	rs := Results(es)
	batch := codecCase{
		name:         string(Batch),
		protoReq:     ProtoBatchRequest(es),
		protoResp:    ProtoBatchResponse(rs),
		jsonReq:      JSONBatchRequest(es),
		jsonResp:     JSONBatchResponse(rs),
		newProtoReq:  func() proto.Message { return ProtoBatchRequest(nil).ProtoReflect().New().Interface() },
		newProtoResp: func() proto.Message { return ProtoBatchResponse(nil).ProtoReflect().New().Interface() },
		newJSONReq:   func() any { return new(httpapi.BatchRequest) },
		newJSONResp:  func() any { return new(httpapi.BatchResponse) },
	}
	return []codecCase{single(Small), single(Large), batch}
}

func benchProtoMarshal(b *testing.B, pick func(codecCase) proto.Message) {
	for _, c := range codecCases() {
		msg := pick(c)
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := proto.Marshal(msg); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func benchProtoUnmarshal(b *testing.B, pick func(codecCase) (proto.Message, func() proto.Message)) {
	for _, c := range codecCases() {
		m, fresh := pick(c)
		raw, err := proto.Marshal(m)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if err := proto.Unmarshal(raw, fresh()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func benchJSONMarshal(b *testing.B, pick func(codecCase) any) {
	for _, c := range codecCases() {
		v := pick(c)
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := json.Marshal(v); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func benchJSONUnmarshal(b *testing.B, pick func(codecCase) (any, func() any)) {
	for _, c := range codecCases() {
		v, fresh := pick(c)
		raw, err := json.Marshal(v)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if err := json.Unmarshal(raw, fresh()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkProtoMarshalRequest(b *testing.B) {
	benchProtoMarshal(b, func(c codecCase) proto.Message { return c.protoReq })
}

func BenchmarkProtoMarshalResponse(b *testing.B) {
	benchProtoMarshal(b, func(c codecCase) proto.Message { return c.protoResp })
}

func BenchmarkProtoUnmarshalRequest(b *testing.B) {
	benchProtoUnmarshal(b, func(c codecCase) (proto.Message, func() proto.Message) { return c.protoReq, c.newProtoReq })
}

func BenchmarkProtoUnmarshalResponse(b *testing.B) {
	benchProtoUnmarshal(b, func(c codecCase) (proto.Message, func() proto.Message) { return c.protoResp, c.newProtoResp })
}

func BenchmarkJSONMarshalRequest(b *testing.B) {
	benchJSONMarshal(b, func(c codecCase) any { return c.jsonReq })
}

func BenchmarkJSONMarshalResponse(b *testing.B) {
	benchJSONMarshal(b, func(c codecCase) any { return c.jsonResp })
}

func BenchmarkJSONUnmarshalRequest(b *testing.B) {
	benchJSONUnmarshal(b, func(c codecCase) (any, func() any) { return c.jsonReq, c.newJSONReq })
}

func BenchmarkJSONUnmarshalResponse(b *testing.B) {
	benchJSONUnmarshal(b, func(c codecCase) (any, func() any) { return c.jsonResp, c.newJSONResp })
}
