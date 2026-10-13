package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"

	corecommand "github.com/1998x-stack/mini-redis/internal/command"
	"github.com/1998x-stack/mini-redis/internal/config"
	"github.com/1998x-stack/mini-redis/internal/resp"
	"github.com/1998x-stack/mini-redis/internal/server"
	"github.com/1998x-stack/mini-redis/internal/store"
	"github.com/1998x-stack/mini-redis/internal/store/versionedmemory"
)

type runningServer struct {
	srv    *server.Server
	cancel context.CancelFunc
}

func main() {
	primaryCfg := baseConfig()
	primaryCfg.ReplicationRole = config.ReplicationPrimary
	primaryCfg.ReplicationAddr = "127.0.0.1:0"
	primary := start(primaryCfg)
	defer primary.close()
	wait("primary replication listener", func() bool { return primary.srv.ReplicationAddr() != nil })

	mustCommand(primary.srv.Addr().String(), "SET", "seed", "1")

	replicaCfg := baseConfig()
	replicaCfg.ReplicationRole = config.ReplicationReplica
	replicaCfg.ReplicaOf = primary.srv.ReplicationAddr().String()
	replicaCfg.ReplicationReconnectDelay = 150 * time.Millisecond
	replica := start(replicaCfg)
	defer replica.close()

	wait("FULLRESYNC convergence", func() bool {
		return sendCommand(replica.srv.Addr().String(), "GET", "seed") == "1"
	})
	printState("after initial sync", primary.srv, replica.srv)
	fmt.Printf("sync counters: %+v  <- FULLRESYNC happened\n", primary.srv.ReplicationStats())

	mustCommand(primary.srv.Addr().String(), "SET", "live", "2")
	wait("live propagation", func() bool {
		return sendCommand(replica.srv.Addr().String(), "GET", "live") == "2"
	})
	fmt.Println("live SET propagated without another snapshot")

	if err := replica.srv.DisconnectReplicationLink(); err != nil {
		panic(err)
	}
	mustCommand(primary.srv.Addr().String(), "SET", "during-gap", "3")
	wait("CONTINUE partial resync", func() bool {
		return primary.srv.ReplicationStats().PartialSyncs >= 1 &&
			sendCommand(replica.srv.Addr().String(), "GET", "during-gap") == "3"
	})
	printState("after reconnect", primary.srv, replica.srv)
	fmt.Printf("sync counters: %+v  <- CONTINUE used retained backlog\n", primary.srv.ReplicationStats())

	readonly := sendCommand(replica.srv.Addr().String(), "SET", "local-write", "blocked")
	fmt.Printf("replica direct write -> %s\n", readonly)
}

func baseConfig() config.Config {
	cfg := config.Default()
	cfg.Addr = "127.0.0.1:0"
	cfg.NetworkEngine = config.NetworkLegacy
	cfg.AppendOnly = false
	cfg.RDBFilename = ""
	cfg.MaxMemory = 0
	cfg.ReplicationBacklogBytes = 1 << 20
	cfg.ReplicationSubscriberFrames = 64
	cfg.ReplicationReconnectDelay = 50 * time.Millisecond
	return cfg
}

func start(cfg config.Config) *runningServer {
	registry := corecommand.NewRegistry()
	corecommand.RegisterCore(registry)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv, err := server.New(cfg, versionedmemory.New(store.RealClock{}), registry, logger)
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		if err := srv.ListenAndServe(ctx); err != nil && ctx.Err() == nil {
			panic(err)
		}
	}()
	wait("client listener", func() bool { return srv.Addr() != nil })
	return &runningServer{srv: srv, cancel: cancel}
}

func (r *runningServer) close() {
	r.cancel()
	_ = r.srv.Close()
}

func wait(name string, fn func() bool) {
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	panic("timeout waiting for " + name)
}

func printState(label string, primary, replica *server.Server) {
	_, primaryID, primaryOffset := primary.ReplicationState()
	_, replicaID, replicaOffset := replica.ReplicationState()
	fmt.Printf("%s:\n", label)
	fmt.Printf("  primary id=%s offset=%d\n", shortID(primaryID), primaryOffset)
	fmt.Printf("  replica id=%s offset=%d\n", shortID(replicaID), replicaOffset)
}

func shortID(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[:8] + "..."
}

func mustCommand(addr string, args ...string) {
	result := sendCommand(addr, args...)
	if strings.HasPrefix(result, "error(") {
		panic(result)
	}
}

func sendCommand(addr string, args ...string) string {
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return "error(" + err.Error() + ")"
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(time.Second))
	if _, err := conn.Write(resp.EncodeCommand(args)); err != nil {
		return "error(" + err.Error() + ")"
	}
	reader := bufio.NewReader(conn)
	prefix, err := reader.ReadByte()
	if err != nil {
		return "error(" + err.Error() + ")"
	}
	line := func() (string, error) {
		s, err := reader.ReadString('\n')
		return strings.TrimSuffix(strings.TrimSuffix(s, "\n"), "\r"), err
	}
	switch prefix {
	case '+', ':':
		s, err := line()
		if err != nil {
			return "error(" + err.Error() + ")"
		}
		return s
	case '-':
		s, _ := line()
		return "error(" + s + ")"
	case '$':
		s, err := line()
		if err != nil {
			return "error(" + err.Error() + ")"
		}
		n, err := strconv.Atoi(s)
		if err != nil {
			return "error(" + err.Error() + ")"
		}
		if n < 0 {
			return "nil"
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(reader, buf); err != nil {
			return "error(" + err.Error() + ")"
		}
		return string(buf[:n])
	default:
		return fmt.Sprintf("error(unexpected RESP prefix %q)", prefix)
	}
}
