package server

import (
	"io"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/1998x-stack/mini-redis/internal/config"
	"github.com/1998x-stack/mini-redis/internal/resp"
)

func readExactly(t *testing.T, conn net.Conn, want string) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
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

func enginesForSessionNetworkTest() []config.NetworkEngine {
	engines := []config.NetworkEngine{config.NetworkLegacy}
	if runtime.GOOS == "linux" {
		engines = append(engines, config.NetworkReactor)
	}
	return engines
}

func TestTransactionProtocolOnBothNetworkEngines(t *testing.T) {
	for _, engine := range enginesForSessionNetworkTest() {
		t.Run(string(engine), func(t *testing.T) {
			cfg := config.Default()
			cfg.Addr = "127.0.0.1:0"
			cfg.AppendOnly = false
			cfg.RDBFilename = ""
			cfg.NetworkEngine = engine
			srv, _ := startTestServer(t, cfg)

			conn, err := net.DialTimeout("tcp", srv.Addr().String(), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()

			request := append([]byte{}, resp.EncodeCommand([]string{"MULTI"})...)
			request = append(request, resp.EncodeCommand([]string{"SET", "k", "v"})...)
			request = append(request, resp.EncodeCommand([]string{"GET", "k"})...)
			request = append(request, resp.EncodeCommand([]string{"EXEC"})...)
			if _, err := conn.Write(request); err != nil {
				t.Fatal(err)
			}
			readExactly(t, conn, "+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n+OK\r\n$1\r\nv\r\n")
		})
	}
}

func TestPubSubProtocolOnBothNetworkEngines(t *testing.T) {
	for _, engine := range enginesForSessionNetworkTest() {
		t.Run(string(engine), func(t *testing.T) {
			cfg := config.Default()
			cfg.Addr = "127.0.0.1:0"
			cfg.AppendOnly = false
			cfg.RDBFilename = ""
			cfg.NetworkEngine = engine
			srv, _ := startTestServer(t, cfg)

			subscriber, err := net.DialTimeout("tcp", srv.Addr().String(), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer subscriber.Close()
			publisher, err := net.DialTimeout("tcp", srv.Addr().String(), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer publisher.Close()

			if _, err := subscriber.Write(resp.EncodeCommand([]string{"SUBSCRIBE", "news"})); err != nil {
				t.Fatal(err)
			}
			readExactly(t, subscriber, "*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n")

			if _, err := publisher.Write(resp.EncodeCommand([]string{"PUBLISH", "news", "hello"})); err != nil {
				t.Fatal(err)
			}
			readExactly(t, publisher, ":1\r\n")
			readExactly(t, subscriber, "*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n")

			if _, err := subscriber.Write(resp.EncodeCommand([]string{"UNSUBSCRIBE", "news"})); err != nil {
				t.Fatal(err)
			}
			readExactly(t, subscriber, "*3\r\n$11\r\nunsubscribe\r\n$4\r\nnews\r\n:0\r\n")
		})
	}
}
