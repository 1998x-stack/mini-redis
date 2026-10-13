package server

import (
	"net"
	"testing"
	"time"

	"github.com/1998x-stack/mini-redis/internal/config"
	"github.com/1998x-stack/mini-redis/internal/resp"
)

func readProtocolValue(t *testing.T, conn net.Conn) resp.Value {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 0, 4096)
	var chunk [512]byte
	for {
		if value, _, complete, err := resp.ParseValue(buf); err != nil {
			t.Fatal(err)
		} else if complete {
			return value
		}
		n, err := conn.Read(chunk[:])
		if err != nil {
			t.Fatal(err)
		}
		buf = append(buf, chunk[:n]...)
	}
}

func writeCommand(t *testing.T, conn net.Conn, argv ...string) {
	t.Helper()
	if _, err := conn.Write(resp.EncodeCommand(argv)); err != nil {
		t.Fatal(err)
	}
}

func mapLookup(value resp.Value, key string) (resp.Value, bool) {
	if value.Type != resp.MapType {
		return resp.Value{}, false
	}
	for _, entry := range value.Map {
		if entry.Key.Type == resp.BulkStringType && entry.Key.Str == key {
			return entry.Value, true
		}
	}
	return resp.Value{}, false
}

func TestRESP3ProtocolOnBothNetworkEngines(t *testing.T) {
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

			writeCommand(t, conn, "HELLO", "3")
			hello := readProtocolValue(t, conn)
			if hello.Type != resp.MapType {
				t.Fatalf("HELLO 3 type=%v", hello.Type)
			}
			proto, ok := mapLookup(hello, "proto")
			if !ok || proto.Type != resp.IntegerType || proto.Int != 3 {
				t.Fatalf("HELLO proto=%#v ok=%v", proto, ok)
			}

			writeCommand(t, conn, "HSET", "h", "a", "1", "b", "2")
			if got := readProtocolValue(t, conn); got.Type != resp.IntegerType || got.Int != 2 {
				t.Fatalf("HSET=%#v", got)
			}
			writeCommand(t, conn, "HGETALL", "h")
			if got := readProtocolValue(t, conn); got.Type != resp.MapType || len(got.Map) != 2 {
				t.Fatalf("HGETALL=%#v", got)
			}

			writeCommand(t, conn, "SADD", "s", "a", "b")
			_ = readProtocolValue(t, conn)
			writeCommand(t, conn, "SMEMBERS", "s")
			if got := readProtocolValue(t, conn); got.Type != resp.SetType || len(got.Array) != 2 {
				t.Fatalf("SMEMBERS=%#v", got)
			}
			writeCommand(t, conn, "SISMEMBER", "s", "a")
			if got := readProtocolValue(t, conn); got.Type != resp.BooleanType || !got.Bool {
				t.Fatalf("SISMEMBER=%#v", got)
			}

			writeCommand(t, conn, "ZADD", "z", "1.25", "member")
			_ = readProtocolValue(t, conn)
			writeCommand(t, conn, "ZSCORE", "z", "member")
			if got := readProtocolValue(t, conn); got.Type != resp.DoubleType || got.Float != 1.25 {
				t.Fatalf("ZSCORE=%#v", got)
			}

			writeCommand(t, conn, "HELLO", "2")
			if got := readProtocolValue(t, conn); got.Type != resp.ArrayType {
				t.Fatalf("HELLO 2 downgrade=%#v", got)
			}
			writeCommand(t, conn, "HGETALL", "h")
			if got := readProtocolValue(t, conn); got.Type != resp.ArrayType || len(got.Array) != 4 {
				t.Fatalf("RESP2 HGETALL=%#v", got)
			}
		})
	}
}

func TestRESP3PubSubUsesPushAndAllowsCommandsWhileSubscribed(t *testing.T) {
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

			writeCommand(t, subscriber, "HELLO", "3")
			_ = readProtocolValue(t, subscriber)
			writeCommand(t, subscriber, "SUBSCRIBE", "news")
			if got := readProtocolValue(t, subscriber); got.Type != resp.PushType {
				t.Fatalf("SUBSCRIBE=%#v", got)
			}

			writeCommand(t, subscriber, "GET", "missing")
			if got := readProtocolValue(t, subscriber); got.Type != resp.NullType {
				t.Fatalf("GET while subscribed=%#v", got)
			}

			writeCommand(t, publisher, "PUBLISH", "news", "hello")
			if got := readProtocolValue(t, publisher); got.Type != resp.IntegerType || got.Int != 1 {
				t.Fatalf("PUBLISH=%#v", got)
			}
			message := readProtocolValue(t, subscriber)
			if message.Type != resp.PushType || len(message.Array) != 3 || message.Array[0].Str != "message" {
				t.Fatalf("message=%#v", message)
			}
		})
	}
}
