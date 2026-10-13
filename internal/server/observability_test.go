package server

import (
	"net"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/1998x-stack/mini-redis/internal/config"
	"github.com/1998x-stack/mini-redis/internal/resp"
)

func readWireValue(t *testing.T, conn net.Conn) resp.Value {
	t.Helper()
	var buffer []byte
	for {
		value, consumed, complete, err := resp.ParseValue(buffer)
		if err != nil {
			t.Fatal(err)
		}
		if complete {
			if consumed != len(buffer) {
				t.Fatalf("unexpected trailing reply bytes: consumed=%d len=%d", consumed, len(buffer))
			}
			return value
		}
		var chunk [4096]byte
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, err := conn.Read(chunk[:])
		if err != nil {
			t.Fatal(err)
		}
		buffer = append(buffer, chunk[:n]...)
	}
}

func observabilityEngines() []config.NetworkEngine {
	engines := []config.NetworkEngine{config.NetworkLegacy}
	if runtime.GOOS == "linux" {
		engines = append(engines, config.NetworkReactor)
	}
	return engines
}

func TestWireMetricsAndInfoOnBothNetworkEngines(t *testing.T) {
	for _, engine := range observabilityEngines() {
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

			for _, argv := range [][]string{
				{"SET", "observed", "1"},
				{"PING"},
				{"HELLO", "3"},
				{"INFO", "commands"},
			} {
				if _, err := conn.Write(resp.EncodeCommand(argv)); err != nil {
					t.Fatal(err)
				}
				reply := readWireValue(t, conn)
				if reply.IsError() {
					t.Fatalf("%v: %s", argv, reply.Str)
				}
			}

			snapshot := srv.MetricsSnapshot()
			label := string(engine)
			if snapshot.Keys != 1 {
				t.Fatalf("INFO mutated database: keys=%d want=1", snapshot.Keys)
			}
			if snapshot.Runtime.Connections[label] == 0 || snapshot.Runtime.ActiveConnections[label] != 1 {
				t.Fatalf("connection metrics=%v active=%v", snapshot.Runtime.Connections, snapshot.Runtime.ActiveConnections)
			}
			if snapshot.Runtime.BytesRead[label] == 0 || snapshot.Runtime.BytesWritten[label] == 0 {
				t.Fatalf("wire bytes read=%v written=%v", snapshot.Runtime.BytesRead, snapshot.Runtime.BytesWritten)
			}
			for _, command := range []string{"SET", "PING", "HELLO", "INFO"} {
				if snapshot.Runtime.Commands[command] != 1 {
					t.Fatalf("%s count=%d want=1", command, snapshot.Runtime.Commands[command])
				}
			}
			if snapshot.Runtime.ProtocolRequests["resp2"] != 3 || snapshot.Runtime.ProtocolRequests["resp3"] != 1 {
				t.Fatalf("protocol requests=%v", snapshot.Runtime.ProtocolRequests)
			}
			if snapshot.Runtime.CommandLatency.Count != 4 {
				t.Fatalf("latency count=%d want=4", snapshot.Runtime.CommandLatency.Count)
			}
			if engine == config.NetworkReactor && snapshot.Runtime.ReactorPollCalls == 0 {
				t.Fatal("reactor poll calls were not observed")
			}
		})
	}
}

func TestMetricsSnapshotDoesNotExposeMutableMetricMaps(t *testing.T) {
	cfg := config.Default()
	cfg.AppendOnly = false
	cfg.RDBFilename = ""
	srv, _ := startTestServer(t, cfg)

	roundTrip(t, srv.Addr(), resp.EncodeCommand([]string{"PING"}), "+PONG\r\n")
	first := srv.MetricsSnapshot()
	first.Runtime.Commands["PING"] = 999
	second := srv.MetricsSnapshot()
	if second.Runtime.Commands["PING"] != 1 {
		t.Fatalf("snapshot mutation leaked into server metrics: %d", second.Runtime.Commands["PING"])
	}
}

func TestPersistenceMetricsMeasureRealFiles(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.AppendOnly = true
	cfg.AppendFilename = filepath.Join(dir, "appendonly.aof")
	cfg.RDBFilename = filepath.Join(dir, "dump.mrdb")
	cfg.AppendFsync = config.AppendFsyncAlways

	registry, st, logger := testServerParts()
	srv, err := New(cfg, st, registry, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	if reply := srv.executeCommand([]string{"SET", "persisted", "value"}); reply.IsError() {
		t.Fatal(reply.Str)
	}
	if err := srv.SaveRDB(); err != nil {
		t.Fatal(err)
	}
	if err := srv.RewriteAOF(); err != nil {
		t.Fatal(err)
	}

	snapshot := srv.MetricsSnapshot().Runtime
	if snapshot.AOFAppendOps == 0 || snapshot.AOFAppendBytes == 0 || snapshot.AOFAppendLatency.Count == 0 {
		t.Fatalf("AOF metrics=%+v", snapshot)
	}
	if snapshot.RDBSaveOps != 1 || snapshot.RDBSaveBytes == 0 || snapshot.RDBSaveLatency.Count != 1 {
		t.Fatalf("RDB metrics=%+v", snapshot)
	}
	if snapshot.AOFRewriteOps != 1 || snapshot.AOFRewriteBytes == 0 || snapshot.AOFRewriteLatency.Count != 1 {
		t.Fatalf("rewrite metrics=%+v", snapshot)
	}
}
