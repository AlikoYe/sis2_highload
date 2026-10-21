package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"sis2_highload/internal/apierr"
	"sis2_highload/internal/domain"
	"sis2_highload/internal/httpapi"
)

type restClient struct {
	caller
	base      string
	http      *http.Client
	transport *http.Transport
}

// NewREST talks HTTP/1.1 to baseURL (e.g. http://localhost:8080).
func NewREST(baseURL string, opts Options) Client { return newREST(baseURL, opts, false) }

// NewRESTH2C talks cleartext HTTP/2 with prior knowledge to baseURL.
func NewRESTH2C(baseURL string, opts Options) Client { return newREST(baseURL, opts, true) }

func newREST(baseURL string, opts Options, h2c bool) *restClient {
	t := &http.Transport{
		DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        1024,
		MaxIdleConnsPerHost: 1024,
		IdleConnTimeout:     90 * time.Second, // below the server's 120s so we never reuse a conn it is closing
		ForceAttemptHTTP2:   false,
	}
	if h2c {
		var p http.Protocols
		p.SetUnencryptedHTTP2(true)
		t.Protocols = &p
	}
	return &restClient{
		caller:    newCaller(opts),
		base:      strings.TrimRight(baseURL, "/"),
		http:      &http.Client{Transport: t},
		transport: t,
	}
}

func (c *restClient) Publish(ctx context.Context, e domain.Event) (domain.Result, error) {
	body, err := json.Marshal(httpapi.EventFromDomain(e))
	if err != nil {
		return domain.Result{}, fmt.Errorf("encode event: %w", err)
	}
	var out httpapi.Result
	err = c.call(ctx, func(ctx context.Context) error {
		return c.do(ctx, http.MethodPost, "/v1/events", body, &out)
	})
	if err != nil {
		return domain.Result{}, err
	}
	return out.ToDomain(), nil
}

func (c *restClient) PublishBatch(ctx context.Context, es []domain.Event) ([]domain.Result, error) {
	req := httpapi.BatchRequest{Events: make([]httpapi.Event, len(es))}
	for i, e := range es {
		req.Events[i] = httpapi.EventFromDomain(e)
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("encode batch: %w", err)
	}
	var out httpapi.BatchResponse
	err = c.call(ctx, func(ctx context.Context) error {
		return c.do(ctx, http.MethodPost, "/v1/events/batch", body, &out)
	})
	if err != nil {
		return nil, err
	}
	res := make([]domain.Result, len(out.Results))
	for i, r := range out.Results {
		res[i] = r.ToDomain()
	}
	return res, nil
}

func (c *restClient) GetStatus(ctx context.Context, eventID string) (domain.Result, error) {
	var out httpapi.Result
	err := c.call(ctx, func(ctx context.Context) error {
		return c.do(ctx, http.MethodGet, "/v1/events/"+url.PathEscape(eventID), nil, &out)
	})
	if err != nil {
		return domain.Result{}, err
	}
	return out.ToDomain(), nil
}

func (c *restClient) Close() error {
	c.transport.CloseIdleConnections()
	return nil
}

// do performs one attempt. body is reused as-is on every attempt.
func (c *restClient) do(ctx context.Context, method, path string, body []byte, out any) error {
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, r)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if deadline, ok := ctx.Deadline(); ok {
		ms := max(1, time.Until(deadline).Milliseconds())
		req.Header.Set(httpapi.TimeoutHeader, strconv.FormatInt(ms, 10))
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return transportError(ctx, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode >= 300 {
		return httpError(resp)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return transportError(ctx, fmt.Errorf("decode response: %w", err))
	}
	return nil
}

// transportError: no (complete) response. Caused by our deadline -> final; otherwise
// (refused, reset) the request may or may not have landed, and retrying is safe
// because the same event_id is resent.
func transportError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return fmt.Errorf("%w: %w", ctxErr, err)
	}
	return fmt.Errorf("%w: transport: %w", domain.ErrUnavailable, err)
}

func httpError(resp *http.Response) error {
	re := &RemoteError{Kind: apierr.FromHTTP(resp.StatusCode)}
	var body httpapi.ErrorResponse
	if json.NewDecoder(resp.Body).Decode(&body) == nil {
		re.Message = body.Error.Message
	}
	if s, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && s > 0 {
		re.RetryAfter = time.Duration(s) * time.Second
	}
	return re
}
