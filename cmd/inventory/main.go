// Command inventory runs the inventory service: seat holds, their deadlines,
// and the availability view.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/AymanKastali/sagaflow/internal/inventory"
)

func main() {
	var cfg inventory.Config
	flag.StringVar(&cfg.DSN, "dsn",
		"postgres://sagaflow:sagaflow@localhost:5434/inventory?sslmode=disable",
		"Postgres connection string for inventory's own database")
	brokers := flag.String("brokers", "localhost:9092",
		"comma-separated Kafka bootstrap servers")
	flag.StringVar(&cfg.Registry, "registry", "http://localhost:8080/apis/ccompat/v7",
		"schema registry ccompat base URL — must include the /apis/ccompat/v7 path")
	flag.Parse()
	cfg.Brokers = strings.Split(*brokers, ",")

	// SIGINT and SIGTERM cancel the context rather than killing the process, so
	// stopping this binary takes the same code path a test takes with cancel().
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg); err != nil {
		slog.Error("inventory stopped", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg inventory.Config) error {
	service, err := inventory.New(ctx, cfg)
	if err != nil {
		return err
	}
	defer service.Close()
	slog.Info("inventory running",
		"commands", inventory.CommandsTopic, "events", inventory.EventsTopic)
	return service.Run(ctx)
}
