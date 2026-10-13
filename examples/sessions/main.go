package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/1998x-stack/mini-redis/internal/command"
	"github.com/1998x-stack/mini-redis/internal/config"
	"github.com/1998x-stack/mini-redis/internal/resp"
	"github.com/1998x-stack/mini-redis/internal/server"
	"github.com/1998x-stack/mini-redis/internal/store"
	"github.com/1998x-stack/mini-redis/internal/store/versionedmemory"
)

type client struct {
	conn net.Conn
	in   *bufio.Reader
}

func dial(addr string) (*client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &client{conn: conn, in: bufio.NewReader(conn)}, nil
}

func (c *client) close() { _ = c.conn.Close() }

func (c *client) command(args ...string) (string, error) {
	if _, err := c.conn.Write(resp.EncodeCommand(args)); err != nil {
		return "", err
	}
	return c.readValue()
}

func (c *client) readValue() (string, error) {
	prefix, err := c.in.ReadByte()
	if err != nil {
		return "", err
	}
	line := func() (string, error) {
		s, err := c.in.ReadString('\n')
		return strings.TrimSuffix(strings.TrimSuffix(s, "\n"), "\r"), err
	}

	switch prefix {
	case '+':
		return line()
	case '-':
		s, err := line()
		return "error(" + s + ")", err
	case ':':
		return line()
	case '$':
		s, err := line()
		if err != nil {
			return "", err
		}
		n, err := strconv.Atoi(s)
		if err != nil {
			return "", err
		}
		if n < 0 {
			return "nil", nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(c.in, buf); err != nil {
			return "", err
		}
		return strconv.Quote(string(buf[:n])), nil
	case '*':
		s, err := line()
		if err != nil {
			return "", err
		}
		n, err := strconv.Atoi(s)
		if err != nil {
			return "", err
		}
		if n < 0 {
			return "nil-array", nil
		}
		items := make([]string, 0, n)
		for i := 0; i < n; i++ {
			item, err := c.readValue()
			if err != nil {
				return "", err
			}
			items = append(items, item)
		}
		return "[" + strings.Join(items, ", ") + "]", nil
	default:
		return "", fmt.Errorf("unknown RESP prefix %q", prefix)
	}
}

func main() {
	cfg := config.Default()
	cfg.Addr = "127.0.0.1:0"
	cfg.AppendOnly = false
	cfg.RDBFilename = ""
	cfg.NetworkEngine = config.NetworkLegacy

	registry := command.NewRegistry()
	command.RegisterCore(registry)
	srv, err := server.New(cfg, versionedmemory.New(store.RealClock{}), registry, nil)
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		if err := srv.ListenAndServe(ctx); err != nil {
			panic(err)
		}
	}()
	defer srv.Close()

	for srv.Addr() == nil {
		time.Sleep(time.Millisecond)
	}
	addr := srv.Addr().String()

	alice, err := dial(addr)
	if err != nil {
		panic(err)
	}
	defer alice.close()
	bob, err := dial(addr)
	if err != nil {
		panic(err)
	}
	defer bob.close()

	fmt.Println("== WATCH optimistic conflict ==")
	mustPrint("bob SET balance 10", bob.command("SET", "balance", "10"))
	mustPrint("alice WATCH balance", alice.command("WATCH", "balance"))
	mustPrint("bob SET balance 11", bob.command("SET", "balance", "11"))
	mustPrint("alice MULTI", alice.command("MULTI"))
	mustPrint("alice SET balance 12", alice.command("SET", "balance", "12"))
	mustPrint("alice EXEC", alice.command("EXEC"))

	fmt.Println("\n== MULTI queues, EXEC runs in order ==")
	mustPrint("MULTI", alice.command("MULTI"))
	mustPrint("SET tx 1", alice.command("SET", "tx", "1"))
	mustPrint("INCR tx", alice.command("INCR", "tx"))
	mustPrint("GET tx", alice.command("GET", "tx"))
	mustPrint("EXEC", alice.command("EXEC"))

	fmt.Println("\n== Pub/Sub changes connection role ==")
	subscriber, err := dial(addr)
	if err != nil {
		panic(err)
	}
	defer subscriber.close()
	publisher, err := dial(addr)
	if err != nil {
		panic(err)
	}
	defer publisher.close()
	mustPrint("SUBSCRIBE news", subscriber.command("SUBSCRIBE", "news"))
	mustPrint("PUBLISH news hello", publisher.command("PUBLISH", "news", "hello"))
	message, err := subscriber.readValue()
	mustPrint("subscriber push", message, err)
	mustPrint("UNSUBSCRIBE news", subscriber.command("UNSUBSCRIBE", "news"))
}

func mustPrint(label string, value string, err error) {
	if err != nil {
		panic(err)
	}
	fmt.Printf("%-28s -> %s\n", label, value)
}
