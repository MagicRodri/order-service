package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/MagicRodri/order-service/internal/api"
	"github.com/MagicRodri/order-service/internal/app"
	"github.com/MagicRodri/order-service/internal/config"
	"github.com/MagicRodri/order-service/internal/eventing"
	"github.com/MagicRodri/order-service/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel()}))
	slog.SetDefault(log)

	if err := run(log); err != nil {
		log.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	pool, err := openDB(ctx, cfg.DatabaseURL, log)
	if err != nil {
		return err
	}
	defer pool.Close()

	st := store.New(pool)
	if err := st.Migrate(ctx); err != nil {
		return err
	}
	log.Info("migrations applied")

	application := app.New(st, log)

	decoder, err := eventing.NewDecoder(cfg.SchemaRegistryURL)
	if err != nil {
		return err
	}

	businessConsumer, err := eventing.NewConsumer(cfg.KafkaBrokers, cfg.ConsumerGroup+"-business",
		cfg.Business, decoder, application.HandleBusinessEvent, log)
	if err != nil {
		return err
	}
	technicalConsumer, err := eventing.NewConsumer(cfg.KafkaBrokers, cfg.ConsumerGroup+"-technical",
		cfg.Technical, decoder, application.HandleTechnicalEvent, log)
	if err != nil {
		return err
	}

	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.New(application, log).Routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		log.Info("consuming business events", "subscription", cfg.Business.String())
		if err := businessConsumer.Run(ctx); err != nil {
			log.Error("business consumer stopped", "error", err)
		}
	}()

	go func() {
		defer wg.Done()
		log.Info("consuming technical events", "subscription", cfg.Technical.String())
		if err := technicalConsumer.Run(ctx); err != nil {
			log.Error("technical consumer stopped", "error", err)
		}
	}()

	go func() {
		defer wg.Done()
		log.Info("http listening", "addr", cfg.HTTPAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server stopped", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown", "error", err)
	}

	wg.Wait()
	return nil
}

// openDB retries because Postgres and this service start together under Compose.
func openDB(ctx context.Context, url string, log *slog.Logger) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}

	var lastErr error
	for attempt := 1; attempt <= 30; attempt++ {
		if lastErr = pool.Ping(ctx); lastErr == nil {
			return pool, nil
		}
		if ctx.Err() != nil {
			pool.Close()
			return nil, ctx.Err()
		}
		log.Info("waiting for database", "attempt", attempt)
		select {
		case <-ctx.Done():
			pool.Close()
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	pool.Close()
	return nil, lastErr
}

func logLevel() slog.Level {
	if os.Getenv("LOG_LEVEL") == "debug" {
		return slog.LevelDebug
	}
	return slog.LevelInfo
}
