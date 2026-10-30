// Command payloadsize reports encoded body sizes and on-the-wire bytes per request
// for gRPC, REST/HTTP1.1 and REST/h2c using the shared workload messages.
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"sis2_highload/internal/client"
	"sis2_highload/internal/grpcapi"
	"sis2_highload/internal/httpapi"
	"sis2_highload/internal/service"
	"sis2_highload/internal/store/memory"
	"sis2_highload/internal/workload"
)

const (
	callTimeout = 5 * time.Second
	idSeed      = 42 // fixed so body sizes are reproducible
)

func main() {
	n := flag.Int("n", 1000, "measured requests per scenario and protocol")
	warmup := flag.Int("warmup", 50, "unmeasured requests per scenario and protocol before counting")
	outDir := flag.String("out-dir", "results", "directory for payload_sizes.md and payload_sizes.csv")
	flag.Parse()

	if err := run(*n, *warmup, *outDir); err != nil {
		fmt.Fprintln(os.Stderr, "payloadsize:", err)
		os.Exit(1)
	}
}

type bodyRow struct {
	scenario                      workload.Kind
	protoReq, jsonReq, gzipReq    int
	protoResp, jsonResp, gzipResp int
}

type wireRow struct {
	scenario       workload.Kind
	protocol       string
	clientToServer float64 // average bytes per request
	serverToClient float64
}

func run(n, warmup int, outDir string) error {
	var bodies []bodyRow
	for _, kind := range workload.Kinds {
		row, err := bodySizes(kind)
		if err != nil {
			return err
		}
		bodies = append(bodies, row)
	}

	wire, err := wireSizes(n, warmup)
	if err != nil {
		return err
	}

	md := markdown(bodies, wire, n)
	fmt.Print(md)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(outDir, "payload_sizes.md"), []byte(md), 0o644); err != nil {
		return err
	}
	return writeCSV(filepath.Join(outDir, "payload_sizes.csv"), bodies, wire)
}

func bodySizes(kind workload.Kind) (bodyRow, error) {
	var protoReq, protoResp proto.Message
	var jsonReq, jsonResp any
	if kind == workload.Batch {
		events := workload.BatchEvents(fixedIDs(workload.BatchSize))
		results := workload.Results(events)
		protoReq, protoResp = workload.ProtoBatchRequest(events), workload.ProtoBatchResponse(results)
		jsonReq, jsonResp = workload.JSONBatchRequest(events), workload.JSONBatchResponse(results)
	} else {
		event := workload.Event(kind, fixedIDs(1)[0])
		result := workload.Result(event)
		protoReq, protoResp = workload.ProtoRequest(event), workload.ProtoResponse(result)
		jsonReq, jsonResp = workload.JSONRequest(event), workload.JSONResponse(result)
	}
	row := bodyRow{scenario: kind, protoReq: proto.Size(protoReq), protoResp: proto.Size(protoResp)}
	var err error
	if row.jsonReq, row.gzipReq, err = jsonSizes(jsonReq); err != nil {
		return row, err
	}
	if row.jsonResp, row.gzipResp, err = jsonSizes(jsonResp); err != nil {
		return row, err
	}
	return row, nil
}

// fixedIDs returns seeded pseudo-random UUIDs: reproducible sizes, but ids that
// do not compress unrealistically well in the gzip column.
func fixedIDs(n int) []string {
	rng := rand.NewChaCha8([32]byte{idSeed})
	ids := make([]string, n)
	for i := range ids {
		ids[i] = uuid.Must(uuid.NewRandomFromReader(rng)).String()
	}
	return ids
}

func jsonSizes(v any) (plain, gzipped int, err error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return 0, 0, err
	}
	var compressed bytes.Buffer
	gz := gzip.NewWriter(&compressed)
	if _, err := gz.Write(raw); err != nil {
		return 0, 0, err
	}
	if err := gz.Close(); err != nil {
		return 0, 0, err
	}
	return len(raw), compressed.Len(), nil
}

// countingListener counts bytes on the server side of every accepted connection.
type countingListener struct {
	net.Listener
	read, written atomic.Int64
}

func (l *countingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &countingConn{Conn: conn, listener: l}, nil
}

func (l *countingListener) reset() { l.read.Store(0); l.written.Store(0) }

type countingConn struct {
	net.Conn
	listener *countingListener
}

func (c *countingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.listener.read.Add(int64(n))
	return n, err
}

func (c *countingConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.listener.written.Add(int64(n))
	return n, err
}

func listen() (*countingListener, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	return &countingListener{Listener: lis}, nil
}

type endpoint struct {
	name     string
	listener *countingListener
	client   client.Client
}

func wireSizes(n, warmup int) ([]wireRow, error) {
	endpoints, stop, err := startEndpoints()
	if err != nil {
		return nil, err
	}
	defer stop()

	var rows []wireRow
	for _, kind := range workload.Kinds {
		for _, ep := range endpoints {
			row, err := measureWire(ep, kind, n, warmup)
			if err != nil {
				return nil, err
			}
			rows = append(rows, row)
		}
	}
	return rows, nil
}

// startEndpoints serves one in-memory service over gRPC, REST/HTTP1.1 and REST/h2c
// on loopback, each behind a byte-counting listener, and returns a client for each.
func startEndpoints() ([]endpoint, func(), error) {
	log := slog.New(slog.DiscardHandler)
	st := memory.New()
	svc := service.New(st)
	handler := httpapi.New(svc, st, log, nil)
	opts := client.Options{CallTimeout: callTimeout}

	var stops []func()
	stopAll := func() {
		for _, stop := range slices.Backward(stops) {
			stop()
		}
	}
	listeners := make([]*countingListener, 3)
	for i := range listeners {
		lis, err := listen()
		if err != nil {
			stopAll()
			return nil, nil, err
		}
		listeners[i] = lis
	}
	grpcLis, httpLis, h2cLis := listeners[0], listeners[1], listeners[2]

	grpcSrv, _ := grpcapi.New(svc, log, nil)
	go func() { _ = grpcSrv.Serve(grpcLis) }()
	stops = append(stops, grpcSrv.Stop)

	httpSrv := &http.Server{Handler: handler}
	go func() { _ = httpSrv.Serve(httpLis) }()
	stops = append(stops, func() { _ = httpSrv.Close() })

	h2cSrv := &http.Server{Handler: handler, Protocols: new(http.Protocols)}
	h2cSrv.Protocols.SetUnencryptedHTTP2(true)
	go func() { _ = h2cSrv.Serve(h2cLis) }()
	stops = append(stops, func() { _ = h2cSrv.Close() })

	grpcClient, err := client.NewGRPC(grpcLis.Addr().String(), opts)
	if err != nil {
		stopAll()
		return nil, nil, err
	}
	endpoints := []endpoint{
		{"gRPC", grpcLis, grpcClient},
		{"REST/HTTP1.1", httpLis, client.NewREST("http://"+httpLis.Addr().String(), opts)},
		{"REST/h2c", h2cLis, client.NewRESTH2C("http://"+h2cLis.Addr().String(), opts)},
	}
	for _, ep := range endpoints {
		stops = append(stops, func() { _ = ep.client.Close() })
	}
	return endpoints, stopAll, nil
}

// measureWire warms the connection up, then averages socket bytes over n requests.
func measureWire(ep endpoint, kind workload.Kind, n, warmup int) (wireRow, error) {
	send := sender(ep.client, kind)
	for range warmup {
		if err := send(); err != nil {
			return wireRow{}, fmt.Errorf("%s %s warm-up: %w", ep.name, kind, err)
		}
	}
	ep.listener.reset()
	for range n {
		if err := send(); err != nil {
			return wireRow{}, fmt.Errorf("%s %s: %w", ep.name, kind, err)
		}
	}
	return wireRow{
		scenario:       kind,
		protocol:       ep.name,
		clientToServer: float64(ep.listener.read.Load()) / float64(n),
		serverToClient: float64(ep.listener.written.Load()) / float64(n),
	}, nil
}

// sender publishes fresh events so every request is a real insert.
func sender(c client.Client, kind workload.Kind) func() error {
	ctx := context.Background()
	if kind == workload.Batch {
		return func() error {
			ids := make([]string, workload.BatchSize)
			for i := range ids {
				ids[i] = client.NewEventID()
			}
			_, err := c.PublishBatch(ctx, workload.BatchEvents(ids))
			return err
		}
	}
	return func() error {
		_, err := c.Publish(ctx, workload.Event(kind, client.NewEventID()))
		return err
	}
}

func markdown(bodies []bodyRow, wire []wireRow, n int) string {
	var b strings.Builder
	b.WriteString("# Payload sizes\n\n")
	b.WriteString("## Body sizes (bytes)\n\n")
	b.WriteString("Encoded message only, no headers or framing. JSON gzip is for discussion; the REST API does not compress.\n\n")
	b.WriteString("| scenario | request proto | request JSON | JSON/proto | request JSON gzip | response proto | response JSON | JSON/proto | response JSON gzip |\n")
	b.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, r := range bodies {
		fmt.Fprintf(&b, "| %s | %d | %d | %.2fx | %d | %d | %d | %.2fx | %d |\n",
			r.scenario, r.protoReq, r.jsonReq, ratio(r.jsonReq, r.protoReq), r.gzipReq,
			r.protoResp, r.jsonResp, ratio(r.jsonResp, r.protoResp), r.gzipResp)
	}
	fmt.Fprintf(&b, "\n## Wire bytes per request (average over %d requests on a warm connection)\n\n", n)
	b.WriteString("Counted on the server socket: includes HTTP/1.1 headers, HTTP/2 frames + HPACK, gRPC framing and trailers; excludes connection setup.\n\n")
	b.WriteString("| scenario | protocol | client→server | server→client | total |\n")
	b.WriteString("|---|---|---:|---:|---:|\n")
	for _, r := range wire {
		fmt.Fprintf(&b, "| %s | %s | %.0f | %.0f | %.0f |\n",
			r.scenario, r.protocol, r.clientToServer, r.serverToClient, r.clientToServer+r.serverToClient)
	}
	return b.String()
}

func ratio(a, b int) float64 { return float64(a) / float64(b) }

func writeCSV(path string, bodies []bodyRow, wire []wireRow) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	_ = w.Write([]string{"kind", "scenario", "protocol", "encoding", "direction", "bytes"})
	itoa := strconv.Itoa
	for _, r := range bodies {
		scenario := string(r.scenario)
		for _, record := range [][]string{
			{"body", scenario, "", "proto", "request", itoa(r.protoReq)},
			{"body", scenario, "", "json", "request", itoa(r.jsonReq)},
			{"body", scenario, "", "json_gzip", "request", itoa(r.gzipReq)},
			{"body", scenario, "", "proto", "response", itoa(r.protoResp)},
			{"body", scenario, "", "json", "response", itoa(r.jsonResp)},
			{"body", scenario, "", "json_gzip", "response", itoa(r.gzipResp)},
		} {
			_ = w.Write(record)
		}
	}
	ftoa := func(v float64) string { return strconv.FormatFloat(v, 'f', 1, 64) }
	for _, r := range wire {
		scenario := string(r.scenario)
		_ = w.Write([]string{"wire", scenario, r.protocol, "", "client_to_server", ftoa(r.clientToServer)})
		_ = w.Write([]string{"wire", scenario, r.protocol, "", "server_to_client", ftoa(r.serverToClient)})
	}
	w.Flush()
	if err := w.Error(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
