//go:build linux

package server

import (
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/1998x-stack/mini-redis/internal/config"
	"github.com/1998x-stack/mini-redis/internal/resp"
)

func reactorConfig() config.Config {
	cfg := config.Default()
	cfg.Addr = "127.0.0.1:0"
	cfg.AppendOnly = false
	cfg.NetworkEngine = config.NetworkReactor
	return cfg
}

func TestReactorPipelinePreservesReplyOrder(t *testing.T) {
	srv, _ := startTestServer(t, reactorConfig())
	conn, err := net.DialTimeout("tcp", srv.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(time.Second))
	var request []byte
	request = append(request, resp.EncodeCommand([]string{"SET", "n", "1"})...)
	request = append(request, resp.EncodeCommand([]string{"INCR", "n"})...)
	request = append(request, resp.EncodeCommand([]string{"GET", "n"})...)
	if _, err := conn.Write(request); err != nil {
		t.Fatal(err)
	}
	want := "+OK\r\n:2\r\n$1\r\n2\r\n"
	buf := make([]byte, len(want))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatal(err)
	}
	if got := string(buf); got != want {
		t.Fatalf("pipeline reply=%q want=%q", got, want)
	}
}

func TestPartialFrameDoesNotBlockAnotherClient(t *testing.T) {
	srv, _ := startTestServer(t, reactorConfig())
	slow, err := net.DialTimeout("tcp", srv.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Close()
	if _, err := slow.Write([]byte("*2\r\n$3\r\nGET\r\n$5\r\nhel")); err != nil {
		t.Fatal(err)
	}
	fast, err := net.DialTimeout("tcp", srv.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer fast.Close()
	_ = fast.SetDeadline(time.Now().Add(500 * time.Millisecond))
	if _, err := fast.Write(resp.EncodeCommand([]string{"PING"})); err != nil {
		t.Fatal(err)
	}
	pong := make([]byte, len("+PONG\r\n"))
	if _, err := io.ReadFull(fast, pong); err != nil {
		t.Fatalf("fast client blocked behind partial frame: %v", err)
	}
	if string(pong) != "+PONG\r\n" {
		t.Fatalf("fast reply=%q", pong)
	}
	_ = slow.SetDeadline(time.Now().Add(time.Second))
	if _, err := slow.Write([]byte("lo\r\n")); err != nil {
		t.Fatal(err)
	}
	null := make([]byte, len("$-1\r\n"))
	if _, err := io.ReadFull(slow, null); err != nil {
		t.Fatal(err)
	}
	if string(null) != "$-1\r\n" {
		t.Fatalf("slow reply=%q", null)
	}
}

func TestOutputBufferLimitDropsSlowClientWithoutHurtingOthers(t *testing.T) {
	cfg := reactorConfig()
	cfg.ClientOutputBufferLimit = 32
	srv, _ := startTestServer(t, cfg)
	slow, err := net.DialTimeout("tcp", srv.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	payload := strings.Repeat("x", 128)
	_ = slow.SetDeadline(time.Now().Add(time.Second))
	if _, err := slow.Write(resp.EncodeCommand([]string{"ECHO", payload})); err != nil {
		t.Fatal(err)
	}
	one := make([]byte, 1)
	if _, err := slow.Read(one); err == nil {
		t.Fatal("slow client should be closed after exceeding output buffer limit")
	}
	_ = slow.Close()
	roundTrip(t, srv.Addr(), resp.EncodeCommand([]string{"PING"}), "+PONG\r\n")
}

func TestMalformedProtocolGetsErrorThenClose(t *testing.T) {
	srv, _ := startTestServer(t, reactorConfig())
	conn, err := net.DialTimeout("tcp", srv.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(time.Second))
	if _, err := conn.Write([]byte("*1\r\n+PING\r\n")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 256)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(buf[:n]), "-ERR RESP protocol error") {
		t.Fatalf("reply=%q", buf[:n])
	}
	_, err = conn.Read(buf)
	if err == nil {
		t.Fatal("connection should close after protocol error")
	}
}
