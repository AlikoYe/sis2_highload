package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"sis2_highload/internal/grpcapi"
	"sis2_highload/internal/httpapi"
	"sis2_highload/internal/service"
	"sis2_highload/internal/store/memory"
	"sis2_highload/internal/store/postgres"
)

type config struct {
	store      string
	dbURL      string
	dbMaxConns int
	grpcAddr   string
	httpAddr   string
	h2cAddr    string
	logLevel   slog.Level
}

type store interface {
	service.Store
	httpapi.Pinger
}

func main() {
	cfg, err := parseConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.logLevel}))
	if err := run(cfg, log); err != nil {
		log.Error("server failed", "err", err)
		os.Exit(1)
	}
}

func parseConfig() (config, error) {
	var cfg config
	var level string
	flag.StringVar(&cfg.store, "store", envOr("STORE", "memory"), "store backend: memory|postgres (env STORE)")
	flag.StringVar(&cfg.dbURL, "db-url", os.Getenv("DATABASE_URL"), "Postgres DSN (env DATABASE_URL)")
	flag.IntVar(&cfg.dbMaxConns, "db-max-conns", envInt("DB_MAX_CONNS", 20), "Postgres pool size (env DB_MAX_CONNS)")
	flag.StringVar(&cfg.grpcAddr, "grpc-addr", envOr("GRPC_ADDR", ":50051"), "gRPC listen address (env GRPC_ADDR)")
	flag.StringVar(&cfg.httpAddr, "http-addr", envOr("HTTP_ADDR", ":8080"), "REST HTTP/1.1 listen address (env HTTP_ADDR)")
	flag.StringVar(&cfg.h2cAddr, "h2c-addr", os.Getenv("H2C_ADDR"), "REST over cleartext HTTP/2 listen address, empty disables (env H2C_ADDR)")
	flag.StringVar(&level, "log-level", envOr("LOG_LEVEL", "info"), "debug|info|warn|error (env LOG_LEVEL)")
	flag.Parse()

	if err := cfg.logLevel.UnmarshalText([]byte(level)); err != nil {
		return cfg, fmt.Errorf("invalid -log-level: %w", err)
	}
	if cfg.store != "memory" && cfg.store != "postgres" {
		return cfg, fmt.Errorf("invalid -store %q: want memory or postgres", cfg.store)
	}
	if cfg.dbMaxConns <= 0 {
		return cfg, fmt.Errorf("invalid -db-max-conns %d: must be > 0", cfg.dbMaxConns)
	}
	if cfg.store == "postgres" && cfg.dbURL == "" {
		return cfg, errors.New("-db-url (or DATABASE_URL) is required with -store=postgres")
	}
	return cfg, nil
}

func run(cfg config, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, closeStore, err := openStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer closeStore()
	log.Info("store ready", "store", cfg.store)

	svc := service.New(st)
	grpcSrv, health := grpcapi.New(svc, log)
	handler := httpapi.New(svc, st, log)

	grpcLis, err := net.Listen("tcp", cfg.grpcAddr)
	if err != nil {
		return fmt.Errorf("grpc listen: %w", err)
	}
	errc := make(chan error, 3)
	go func() { errc <- grpcSrv.Serve(grpcLis) }()
	log.Info("listening", "proto", "grpc", "addr", grpcLis.Addr().String())

	httpSrv := newHTTPServer(handler, nil)
	if err := serveHTTP(httpSrv, cfg.httpAddr, errc); err != nil {
		return err
	}
	log.Info("listening", "proto", "http/1.1", "addr", httpSrv.Addr)

	var h2cSrv *http.Server
	if cfg.h2cAddr != "" {
		var protocols http.Protocols
		protocols.SetUnencryptedHTTP2(true)
		h2cSrv = newHTTPServer(handler, &protocols)
		if err := serveHTTP(h2cSrv, cfg.h2cAddr, errc); err != nil {
			return err
		}
		log.Info("listening", "proto", "h2c", "addr", h2cSrv.Addr)
	}

	grpcapi.SetServing(health, true)

	select {
	case <-ctx.Done():
		log.Info("shutting down")
	case err = <-errc:
		log.Error("listener failed, shutting down", "err", err)
	}

	grpcapi.SetServing(health, false)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	grpcDone := make(chan struct{})
	go func() {
		grpcSrv.GracefulStop()
		close(grpcDone)
	}()
	for _, srv := range []*http.Server{httpSrv, h2cSrv} {
		if srv == nil {
			continue
		}
		if serr := srv.Shutdown(shutdownCtx); serr != nil {
			log.Error("http shutdown", "addr", srv.Addr, "err", serr)
		}
	}
	select {
	case <-grpcDone:
	case <-shutdownCtx.Done():
		grpcSrv.Stop()
	}
	log.Info("stopped")
	return err
}

func openStore(ctx context.Context, cfg config) (store, func(), error) {
	if cfg.store == "memory" {
		return memory.New(), func() {}, nil
	}
	poolCfg, err := pgxpool.ParseConfig(cfg.dbURL)
	if err != nil {
		return nil, nil, fmt.Errorf("parse db url: %w", err)
	}
	poolCfg.MaxConns = int32(cfg.dbMaxConns)
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, nil, fmt.Errorf("connect postgres: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, nil, fmt.Errorf("ping postgres: %w", err)
	}
	return postgres.New(pool), pool.Close, nil
}

func newHTTPServer(h http.Handler, protocols *http.Protocols) *http.Server {
	return &http.Server{
		Handler:           h,
		Protocols:         protocols,
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
}

// serveHTTP binds synchronously so address errors fail startup instead of surfacing later.
func serveHTTP(srv *http.Server, addr string, errc chan<- error) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("http listen %s: %w", addr, err)
	}
	srv.Addr = lis.Addr().String()
	go func() {
		if err := srv.Serve(lis); !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()
	return nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if n, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return n
	}
	return def
}
