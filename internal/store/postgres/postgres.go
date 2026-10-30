package postgres

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"sis2_highload/internal/domain"
	"sis2_highload/internal/service"
)

const insertSQL = `
INSERT INTO deliveries (event_id, event_type, user_id, channel, payload, occurred_at)
SELECT event_id, event_type, user_id, channel, payload::jsonb, occurred_at
FROM unnest($1::uuid[], $2::text[], $3::bigint[], $4::text[], $5::text[], $6::timestamptz[])
     AS input(event_id, event_type, user_id, channel, payload, occurred_at)
ON CONFLICT (event_id, channel) DO NOTHING
RETURNING event_id, channel, status, attempts`

const selectByIDsSQL = `
SELECT event_id, channel, status, attempts
FROM deliveries
WHERE event_id = ANY($1::uuid[])`

const selectByIDSQL = `
SELECT event_id, channel, status, attempts
FROM deliveries
WHERE event_id = $1
ORDER BY channel`

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

type deliveryKey struct {
	eventID string
	channel domain.Channel
}

// insertRow is one (event, channel) pair to insert.
type insertRow struct {
	deliveryKey
	event *domain.Event
}

// insertColumns holds insertSQL's parameters: one array per column, one element per row.
type insertColumns struct {
	eventIDs, eventTypes, channels, payloads []string
	userIDs                                  []int64
	occurredAt                               []time.Time
}

func newInsertColumns(rows []insertRow) (insertColumns, error) {
	cols := insertColumns{
		eventIDs:   make([]string, len(rows)),
		eventTypes: make([]string, len(rows)),
		channels:   make([]string, len(rows)),
		payloads:   make([]string, len(rows)),
		userIDs:    make([]int64, len(rows)),
		occurredAt: make([]time.Time, len(rows)),
	}
	for i, row := range rows {
		payload, err := marshalPayload(row.event.Payload)
		if err != nil {
			return insertColumns{}, err
		}
		cols.eventIDs[i] = row.eventID
		cols.eventTypes[i] = row.event.Type
		cols.channels[i] = string(row.channel)
		cols.payloads[i] = payload
		cols.userIDs[i] = row.event.UserID
		cols.occurredAt[i] = row.event.OccurredAt
	}
	return cols, nil
}

func (s *Store) CreateDeliveries(ctx context.Context, items []service.NewEvent) ([]domain.Result, error) {
	rows := uniqueRows(items)
	cols, err := newInsertColumns(rows)
	if err != nil {
		return nil, err
	}

	found := make(map[deliveryKey]domain.Delivery, len(rows))
	err = s.queryInto(ctx, found, insertSQL,
		cols.eventIDs, cols.eventTypes, cols.userIDs, cols.channels, cols.payloads, cols.occurredAt)
	if err != nil {
		return nil, fmt.Errorf("insert deliveries: %w", err)
	}
	// Rows that already existed are not returned by ON CONFLICT DO NOTHING; read them back.
	if len(found) < len(rows) {
		if err := s.queryInto(ctx, found, selectByIDsSQL, missingIDs(rows, found)); err != nil {
			return nil, fmt.Errorf("select existing deliveries: %w", err)
		}
	}
	return resultsInOrder(items, found)
}

func resultsInOrder(items []service.NewEvent, found map[deliveryKey]domain.Delivery) ([]domain.Result, error) {
	results := make([]domain.Result, len(items))
	for i, item := range items {
		deliveries := make([]domain.Delivery, 0, len(item.Channels))
		for _, ch := range item.Channels {
			d, ok := found[deliveryKey{item.Event.ID, ch}]
			if !ok {
				return nil, fmt.Errorf("delivery %s/%s missing after insert", item.Event.ID, ch)
			}
			deliveries = append(deliveries, d)
		}
		results[i] = domain.NewResult(item.Event.ID, deliveries)
	}
	return results, nil
}

func (s *Store) GetDeliveries(ctx context.Context, eventID string) ([]domain.Delivery, error) {
	rows, err := s.pool.Query(ctx, selectByIDSQL, eventID)
	if err != nil {
		return nil, fmt.Errorf("select deliveries: %w", err)
	}
	deliveries, err := pgx.CollectRows(rows, pgx.RowToStructByPos[domain.Delivery])
	if err != nil {
		return nil, fmt.Errorf("select deliveries: %w", err)
	}
	if len(deliveries) == 0 {
		return nil, fmt.Errorf("%w: event %s", domain.ErrNotFound, eventID)
	}
	return deliveries, nil
}

// queryInto runs a query returning delivery rows and adds them to into.
func (s *Store) queryInto(ctx context.Context, into map[deliveryKey]domain.Delivery, sql string, args ...any) error {
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return err
	}
	deliveries, err := pgx.CollectRows(rows, pgx.RowToStructByPos[domain.Delivery])
	if err != nil {
		return err
	}
	for _, d := range deliveries {
		into[deliveryKey{d.EventID, d.Channel}] = d
	}
	return nil
}

// uniqueRows dedupes (event_id, channel) pairs and sorts them so concurrent
// batches take unique-index locks in the same order and cannot deadlock.
func uniqueRows(items []service.NewEvent) []insertRow {
	seen := make(map[deliveryKey]bool)
	var rows []insertRow
	for i := range items {
		event := &items[i].Event
		for _, ch := range items[i].Channels {
			key := deliveryKey{event.ID, ch}
			if seen[key] {
				continue
			}
			seen[key] = true
			rows = append(rows, insertRow{deliveryKey: key, event: event})
		}
	}
	slices.SortFunc(rows, func(a, b insertRow) int {
		return cmp.Or(cmp.Compare(a.eventID, b.eventID), cmp.Compare(a.channel, b.channel))
	})
	return rows
}

func missingIDs(rows []insertRow, found map[deliveryKey]domain.Delivery) []string {
	var ids []string
	for _, row := range rows {
		if _, ok := found[row.deliveryKey]; ok {
			continue
		}
		// rows are sorted by event_id, so a repeat can only be the last one appended.
		if len(ids) == 0 || ids[len(ids)-1] != row.eventID {
			ids = append(ids, row.eventID)
		}
	}
	return ids
}

func marshalPayload(payload map[string]string) (string, error) {
	if len(payload) == 0 {
		return "{}", nil
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal payload: %w", err)
	}
	return string(b), nil
}

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }
