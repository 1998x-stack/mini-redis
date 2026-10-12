package server

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"

	"github.com/1998x-stack/mini-redis/internal/command"
	"github.com/1998x-stack/mini-redis/internal/config"
	"github.com/1998x-stack/mini-redis/internal/resp"
	"github.com/1998x-stack/mini-redis/internal/store"
	"github.com/1998x-stack/mini-redis/internal/store/versionedmemory"
)

func TestConnectionRoundTrip(t *testing.T) {
	cfg := config.Default()
	cfg.AppendOnly = false

	registry := command.NewRegistry()
	command.RegisterCore(registry)
	st := versionedmemory.New(store.RealClock{})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv, err := New(cfg, st, registry, logger)
	if err != nil {
		t.Fatal(err)
	}

	serverConn, clientConn := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.handleConnection(ctx, serverConn)
	defer clientConn.Close()

	if _, err := clientConn.Write(resp.EncodeCommand([]string{"SET", "hello", "world"})); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 5)
	if _, err := io.ReadFull(clientConn, buf); err != nil {
		t.Fatal(err)
	}
	if got := string(buf); got != "+OK\r\n" {
		t.Fatalf("SET reply=%q", got)
	}

	if _, err := clientConn.Write(resp.EncodeCommand([]string{"GET", "hello"})); err != nil {
		t.Fatal(err)
	}
	buf = make([]byte, len("$5\r\nworld\r\n"))
	if _, err := io.ReadFull(clientConn, buf); err != nil {
		t.Fatal(err)
	}
	if got := string(buf); got != "$5\r\nworld\r\n" {
		t.Fatalf("GET reply=%q", got)
	}
}
