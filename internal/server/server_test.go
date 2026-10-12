package server

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/1998x-stack/mini-redis/internal/command"
	"github.com/1998x-stack/mini-redis/internal/config"
	"github.com/1998x-stack/mini-redis/internal/resp"
	"github.com/1998x-stack/mini-redis/internal/store"
	"github.com/1998x-stack/mini-redis/internal/store/versionedmemory"
)

func startTestServer(t *testing.T, cfg config.Config) (*Server, <-chan error) {
	t.Helper()
	registry := command.NewRegistry()
	command.RegisterCore(registry)
	st := versionedmemory.New(store.RealClock{})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv, err := New(cfg, st, registry, logger)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe(ctx) }()
	deadline := time.Now().Add(2 * time.Second)
	for srv.Addr() == nil && time.Now().Before(deadline) {
		select {
		case err := <-errCh:
			t.Fatalf("server exited before listening: %v", err)
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if srv.Addr() == nil {
		t.Fatal("server did not start listening")
	}
	t.Cleanup(func() {
		cancel()
		if err := srv.Close(); err != nil {
			t.Errorf("close server: %v", err)
		}
		select {
		case err := <-errCh:
			if err != nil {
				t.Errorf("ListenAndServe: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("ListenAndServe did not return")
		}
	})
	return srv, errCh
}

func roundTrip(t *testing.T, addr net.Addr, request []byte, want string) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(request); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, len(want))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatal(err)
	}
	if got := string(buf); got != want {
		t.Fatalf("reply=%q want=%q", got, want)
	}
}

func TestLegacyEngineRoundTrip(t *testing.T) {
	cfg := config.Default()
	cfg.Addr = "127.0.0.1:0"
	cfg.AppendOnly = false
	cfg.NetworkEngine = config.NetworkLegacy
	srv, _ := startTestServer(t, cfg)
	roundTrip(t, srv.Addr(), resp.EncodeCommand([]string{"PING"}), "+PONG\r\n")
}
