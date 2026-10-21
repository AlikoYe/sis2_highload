package postgres_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"sis2_highload/internal/store/postgres"
	"sis2_highload/internal/store/storetest"
)

type counted struct {
	*postgres.Store
	pool *pgxpool.Pool
}

func (c counted) RowCount(t *testing.T) int {
	t.Helper()
	var n int
	if err := c.pool.QueryRow(context.Background(), "SELECT count(*) FROM deliveries").Scan(&n); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	return n
}

func TestStore(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set")
	}
	storetest.Run(t, func(t *testing.T) storetest.Store {
		ctx := context.Background()
		cfg, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			t.Fatalf("parse DATABASE_URL: %v", err)
		}
		cfg.MaxConns = 60 // enough for the 50-goroutine duplicate test to hit the DB concurrently
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		t.Cleanup(pool.Close)
		if _, err := pool.Exec(ctx, "TRUNCATE deliveries RESTART IDENTITY"); err != nil {
			t.Fatalf("truncate: %v", err)
		}
		return counted{postgres.New(pool), pool}
	})
}
