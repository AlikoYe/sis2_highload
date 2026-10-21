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
SELECT e, t, u, c, p::jsonb, o
FROM unnest($1::uuid[], $2::text[], $3::bigint[], $4::text[], $5::text[], $6::timestamptz[])
     AS x(e, t, u, c, p, o)
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

type key struct {
	eventID string
	channel domain.Channel
}

type row struct {
	key
	ev *domain.Event
}

func (s *Store) CreateDeliveries(ctx context.Context, items []service.NewEvent) ([]domain.Result, error) {
	rows := uniqueRows(items)
	n := len(rows)
	ids, types, channels, payloads := make([]string, n), make([]string, n), make([]string, n), make([]string, n)
	userIDs, occurred := make([]int64, n), make([]time.Time, n)
	for i, r := range rows {
		ids[i], types[i], channels[i] = r.eventID, r.ev.Type, string(r.channel)
		userIDs[i], occurred[i] = r.ev.UserID, r.ev.OccurredAt
		p, err := marshalPayload(r.ev.Payload)
		if err != nil {
			return nil, err
		}
		payloads[i] = p
	}

	found := make(map[key]domain.Delivery, n)
	if err := s.query(ctx, found, insertSQL, ids, types, userIDs, channels, payloads, occurred); err != nil {
		return nil, fmt.Errorf("insert deliveries: %w", err)
	}
	if len(found) < n {
		if err := s.query(ctx, found, selectByIDsSQL, missingIDs(rows, found)); err != nil {
			return nil, fmt.Errorf("select existing deliveries: %w", err)
		}
	}

	results := make([]domain.Result, len(items))
	for i, it := range items {
		ds := make([]domain.Delivery, 0, len(it.Channels))
		for _, ch := range it.Channels {
			d, ok := found[key{it.Event.ID, ch}]
			if !ok {
				return nil, fmt.Errorf("delivery %s/%s missing after insert", it.Event.ID, ch)
			}
			ds = append(ds, d)
		}
		results[i] = domain.NewResult(it.Event.ID, ds)
	}
	return results, nil
}

func (s *Store) GetDeliveries(ctx context.Context, eventID string) ([]domain.Delivery, error) {
	rows, err := s.pool.Query(ctx, selectByIDSQL, eventID)
	if err != nil {
		return nil, fmt.Errorf("select deliveries: %w", err)
	}
	ds, err := pgx.CollectRows(rows, pgx.RowToStructByPos[domain.Delivery])
	if err != nil {
		return nil, fmt.Errorf("select deliveries: %w", err)
	}
	if len(ds) == 0 {
		return nil, fmt.Errorf("%w: event %s", domain.ErrNotFound, eventID)
	}
	return ds, nil
}

func (s *Store) query(ctx context.Context, into map[key]domain.Delivery, sql string, args ...any) error {
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return err
	}
	ds, err := pgx.CollectRows(rows, pgx.RowToStructByPos[domain.Delivery])
	if err != nil {
		return err
	}
	for _, d := range ds {
		into[key{d.EventID, d.Channel}] = d
	}
	return nil
}

// uniqueRows dedupes (event_id, channel) pairs and sorts them so concurrent
// batches take unique-index locks in the same order and cannot deadlock.
func uniqueRows(items []service.NewEvent) []row {
	seen := make(map[key]bool)
	var rows []row
	for i := range items {
		ev := &items[i].Event
		for _, ch := range items[i].Channels {
			k := key{ev.ID, ch}
			if seen[k] {
				continue
			}
			seen[k] = true
			rows = append(rows, row{key: k, ev: ev})
		}
	}
	slices.SortFunc(rows, func(a, b row) int {
		return cmp.Or(cmp.Compare(a.eventID, b.eventID), cmp.Compare(a.channel, b.channel))
	})
	return rows
}

func missingIDs(rows []row, found map[key]domain.Delivery) []string {
	var ids []string
	for _, r := range rows {
		// rows are sorted by event_id, so a repeat can only be the last one appended.
		if _, ok := found[r.key]; !ok && (len(ids) == 0 || ids[len(ids)-1] != r.eventID) {
			ids = append(ids, r.eventID)
		}
	}
	return ids
}

func marshalPayload(p map[string]string) (string, error) {
	if len(p) == 0 {
		return "{}", nil
	}
	b, err := json.Marshal(p)
	if err != nil {
		return "", fmt.Errorf("marshal payload: %w", err)
	}
	return string(b), nil
}

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }
