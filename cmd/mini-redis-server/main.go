package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/1998x-stack/mini-redis/internal/command"
	"github.com/1998x-stack/mini-redis/internal/config"
	"github.com/1998x-stack/mini-redis/internal/server"
	"github.com/1998x-stack/mini-redis/internal/store"
	"github.com/1998x-stack/mini-redis/internal/store/versionedmemory"
	"github.com/1998x-stack/mini-redis/internal/version"
)

func main() {
	cfg, err := config.ParseFlags()
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(2)
	}
	if cfg.ShowVersion {
		fmt.Println("mini-redis-server v" + version.Version)
		return
	}

	registry := command.NewRegistry()
	command.RegisterCore(registry)
	memory := versionedmemory.New(store.RealClock{})

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	srv, err := server.New(cfg, memory, registry, logger)
	if err != nil {
		logger.Error("create server", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()

	if err := srv.ListenAndServe(ctx); err != nil && ctx.Err() == nil {
		logger.Error("server stopped", "error", err)
		_ = srv.Close()
		os.Exit(1)
	}
}
