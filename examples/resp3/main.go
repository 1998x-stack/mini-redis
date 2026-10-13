package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/1998x-stack/mini-redis/internal/command"
	"github.com/1998x-stack/mini-redis/internal/config"
	"github.com/1998x-stack/mini-redis/internal/resp"
	"github.com/1998x-stack/mini-redis/internal/server"
	"github.com/1998x-stack/mini-redis/internal/store"
	"github.com/1998x-stack/mini-redis/internal/store/versionedmemory"
)

type wireReader struct {
	conn net.Conn
	buf  []byte
}

func (r *wireReader) next() resp.Value {
	for {
		value, consumed, complete, err := resp.ParseValue(r.buf)
		must(err)
		if complete {
			r.buf = append(r.buf[:0], r.buf[consumed:]...)
			return value
		}
		var chunk [1024]byte
		_ = r.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, err := r.conn.Read(chunk[:])
		must(err)
		r.buf = append(r.buf, chunk[:n]...)
	}
}

func main() {
	cfg := config.Default()
	cfg.Addr = "127.0.0.1:0"
	cfg.NetworkEngine = config.NetworkLegacy
	cfg.AppendOnly = false
	cfg.RDBFilename = ""

	registry := command.NewRegistry()
	command.RegisterCore(registry)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv, err := server.New(cfg, versionedmemory.New(store.RealClock{}), registry, logger)
	must(err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = srv.ListenAndServe(ctx) }()
	for srv.Addr() == nil {
		time.Sleep(5 * time.Millisecond)
	}
	defer srv.Close()

	client := dial(srv.Addr().String())
	defer client.Close()
	reader := &wireReader{conn: client}

	fmt.Println("== mini-redis v0.11 RESP3 ==")

	send(client, "HSET", "profile", "name", "redis", "lang", "go")
	_ = reader.next()
	send(client, "HGETALL", "profile")
	fmt.Printf("RESP2 HGETALL       → %s\n", typeName(reader.next()))

	send(client, "HELLO", "3")
	fmt.Printf("HELLO 3             → %s\n", typeName(reader.next()))

	send(client, "HGETALL", "profile")
	fmt.Printf("RESP3 HGETALL       → %s\n", typeName(reader.next()))
	send(client, "SADD", "tags", "storage", "protocol")
	_ = reader.next()
	send(client, "SMEMBERS", "tags")
	fmt.Printf("RESP3 SMEMBERS      → %s\n", typeName(reader.next()))
	send(client, "SISMEMBER", "tags", "storage")
	fmt.Printf("RESP3 SISMEMBER     → %s\n", typeName(reader.next()))
	send(client, "ZADD", "scores", "1.25", "member")
	_ = reader.next()
	send(client, "ZSCORE", "scores", "member")
	fmt.Printf("RESP3 ZSCORE        → %s\n", typeName(reader.next()))

	send(client, "SUBSCRIBE", "news")
	fmt.Printf("RESP3 SUBSCRIBE     → %s\n", typeName(reader.next()))

	publisher := dial(srv.Addr().String())
	publisherReader := &wireReader{conn: publisher}
	send(publisher, "PUBLISH", "news", "hello-push")
	_ = publisherReader.next()
	_ = publisher.Close()
	fmt.Printf("RESP3 async message → %s\n", typeName(reader.next()))

	send(client, "GET", "missing")
	fmt.Printf("command while sub'd → %s\n", typeName(reader.next()))

	send(client, "HELLO", "2")
	fmt.Printf("HELLO 2             → %s (map downgraded to flat array)\n", typeName(reader.next()))

	fmt.Println("mental model: command semantics stay stable; only the connection's representation changes")
}

func dial(addr string) net.Conn {
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	must(err)
	return conn
}

func send(conn net.Conn, argv ...string) {
	_, err := conn.Write(resp.EncodeCommand(argv))
	must(err)
}

func typeName(v resp.Value) string {
	switch v.Type {
	case resp.ArrayType:
		return "Array (*)"
	case resp.MapType:
		return "Map (%)"
	case resp.SetType:
		return "Set (~)"
	case resp.PushType:
		return "Push (>)"
	case resp.BooleanType:
		return "Boolean (#)"
	case resp.DoubleType:
		return "Double (,)"
	case resp.NullType:
		return "Null (_)"
	default:
		return fmt.Sprintf("Type(%d)", v.Type)
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
