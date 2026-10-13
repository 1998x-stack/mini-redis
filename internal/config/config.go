package config

import (
	"flag"
	"fmt"
	"runtime"
	"time"

	"github.com/1998x-stack/mini-redis/internal/aof"
	"github.com/1998x-stack/mini-redis/internal/store"
)

type NetworkEngine string

const (
	NetworkAuto    NetworkEngine = "auto"
	NetworkReactor NetworkEngine = "reactor"
	NetworkLegacy  NetworkEngine = "legacy"
)

type ReplicationRole string

const (
	ReplicationStandalone ReplicationRole = "standalone"
	ReplicationPrimary    ReplicationRole = "primary"
	ReplicationReplica    ReplicationRole = "replica"
)

type Config struct {
	ShowVersion             bool
	Addr                    string
	NetworkEngine           NetworkEngine
	ClientInputBufferLimit  int
	ClientOutputBufferLimit int
	AppendOnly              bool
	AppendFilename          string
	AppendFsync             aof.FsyncPolicy
	RDBFilename             string
	ActiveExpireEvery       time.Duration
	ActiveExpireSample      int
	RehashWorkPerCycle      int
	IdleTimeout             time.Duration
	MaxMemory               int64
	MaxMemoryPolicy         store.EvictionPolicy
	MaxMemorySamples        int
	LFUDecay                time.Duration
	EvictionSeed            int64

	ReplicationRole             ReplicationRole
	ReplicationAddr             string
	ReplicaOf                   string
	ReplicationBacklogBytes     int
	ReplicationSubscriberFrames int
	ReplicationReconnectDelay   time.Duration
}

func Default() Config {
	memory := store.DefaultMemoryConfig()
	return Config{
		Addr:                        "127.0.0.1:6379",
		NetworkEngine:               NetworkAuto,
		ClientInputBufferLimit:      64 << 20,
		ClientOutputBufferLimit:     8 << 20,
		AppendOnly:                  true,
		AppendFilename:              "appendonly.aof",
		AppendFsync:                 aof.FsyncEverySec,
		RDBFilename:                 "dump.mrdb",
		ActiveExpireEvery:           100 * time.Millisecond,
		ActiveExpireSample:          64,
		RehashWorkPerCycle:          64,
		IdleTimeout:                 0,
		MaxMemory:                   memory.MaxBytes,
		MaxMemoryPolicy:             memory.Policy,
		MaxMemorySamples:            memory.Samples,
		LFUDecay:                    memory.LFUDecay,
		EvictionSeed:                memory.RandomSeed,
		ReplicationRole:             ReplicationStandalone,
		ReplicationAddr:             "127.0.0.1:6380",
		ReplicationBacklogBytes:     1 << 20,
		ReplicationSubscriberFrames: 1024,
		ReplicationReconnectDelay:   200 * time.Millisecond,
	}
}

func (c Config) EffectiveNetworkEngine() NetworkEngine {
	if c.NetworkEngine != NetworkAuto {
		return c.NetworkEngine
	}
	if runtime.GOOS == "linux" {
		return NetworkReactor
	}
	return NetworkLegacy
}

func (c Config) MemoryConfig(clock store.Clock) store.MemoryConfig {
	return store.MemoryConfig{
		MaxBytes:   c.MaxMemory,
		Policy:     c.MaxMemoryPolicy,
		Samples:    c.MaxMemorySamples,
		LFUDecay:   c.LFUDecay,
		RandomSeed: c.EvictionSeed,
		Clock:      clock,
	}
}

func ParseFlags() (Config, error) {
	cfg := Default()
	fsync := string(cfg.AppendFsync)
	engine := string(cfg.NetworkEngine)
	policy := string(cfg.MaxMemoryPolicy)
	role := string(cfg.ReplicationRole)

	flag.BoolVar(&cfg.ShowVersion, "version", false, "print version and exit")
	flag.StringVar(&cfg.Addr, "addr", cfg.Addr, "listen address")
	flag.StringVar(&engine, "network-engine", engine, "network engine: auto|reactor|legacy")
	flag.IntVar(&cfg.ClientInputBufferLimit, "client-input-buffer-limit", cfg.ClientInputBufferLimit, "max buffered request bytes per client")
	flag.IntVar(&cfg.ClientOutputBufferLimit, "client-output-buffer-limit", cfg.ClientOutputBufferLimit, "max queued response bytes per client")
	flag.BoolVar(&cfg.AppendOnly, "appendonly", cfg.AppendOnly, "enable AOF persistence; when enabled AOF is authoritative at startup")
	flag.StringVar(&cfg.AppendFilename, "appendfilename", cfg.AppendFilename, "AOF file path")
	flag.StringVar(&fsync, "appendfsync", fsync, "AOF fsync policy: always|everysec|no")
	flag.StringVar(&cfg.RDBFilename, "dbfilename", cfg.RDBFilename, "RDB-like snapshot path; loaded at startup only when appendonly=false")
	flag.DurationVar(&cfg.ActiveExpireEvery, "active-expire-every", cfg.ActiveExpireEvery, "active expiration cycle interval")
	flag.IntVar(&cfg.ActiveExpireSample, "active-expire-sample", cfg.ActiveExpireSample, "max TTL entries checked per cycle; <=0 full scan")
	flag.IntVar(&cfg.RehashWorkPerCycle, "rehash-work-per-cycle", cfg.RehashWorkPerCycle, "bounded dictionary rehash work units per maintenance cycle; 0 disables timer-driven work")
	flag.DurationVar(&cfg.IdleTimeout, "idle-timeout", cfg.IdleTimeout, "connection idle timeout; 0 disables")
	flag.Int64Var(&cfg.MaxMemory, "maxmemory", cfg.MaxMemory, "educational approximate memory limit in bytes; 0 disables eviction")
	flag.StringVar(&policy, "maxmemory-policy", policy, "memory policy: noeviction|allkeys-random|allkeys-lru|allkeys-lfu")
	flag.IntVar(&cfg.MaxMemorySamples, "maxmemory-samples", cfg.MaxMemorySamples, "candidate sample size for approximate LRU/LFU")
	flag.DurationVar(&cfg.LFUDecay, "lfu-decay", cfg.LFUDecay, "LFU counter decay period")
	flag.Int64Var(&cfg.EvictionSeed, "eviction-seed", cfg.EvictionSeed, "deterministic teaching RNG seed for eviction sampling")

	flag.StringVar(&role, "replication-role", role, "replication role: standalone|primary|replica")
	flag.StringVar(&cfg.ReplicationAddr, "replication-addr", cfg.ReplicationAddr, "primary replication listen address")
	flag.StringVar(&cfg.ReplicaOf, "replicaof", cfg.ReplicaOf, "upstream primary replication address for replica role")
	flag.IntVar(&cfg.ReplicationBacklogBytes, "repl-backlog-bytes", cfg.ReplicationBacklogBytes, "bounded primary replication backlog size in canonical stream bytes")
	flag.IntVar(&cfg.ReplicationSubscriberFrames, "repl-subscriber-frames", cfg.ReplicationSubscriberFrames, "max buffered live replication frames per replica connection")
	flag.DurationVar(&cfg.ReplicationReconnectDelay, "repl-reconnect-delay", cfg.ReplicationReconnectDelay, "delay before replica reconnect attempts")
	flag.Parse()

	cfg.NetworkEngine = NetworkEngine(engine)
	switch cfg.NetworkEngine {
	case NetworkAuto, NetworkReactor, NetworkLegacy:
	default:
		return Config{}, fmt.Errorf("invalid network-engine %q", engine)
	}
	if cfg.ClientInputBufferLimit <= 0 {
		return Config{}, fmt.Errorf("client-input-buffer-limit must be positive")
	}
	if cfg.ClientOutputBufferLimit <= 0 {
		return Config{}, fmt.Errorf("client-output-buffer-limit must be positive")
	}

	cfg.AppendFsync = aof.FsyncPolicy(fsync)
	switch cfg.AppendFsync {
	case aof.FsyncAlways, aof.FsyncEverySec, aof.FsyncNo:
	default:
		return Config{}, fmt.Errorf("invalid appendfsync %q", fsync)
	}
	if cfg.ActiveExpireEvery <= 0 {
		return Config{}, fmt.Errorf("active-expire-every must be positive")
	}
	if cfg.RehashWorkPerCycle < 0 {
		return Config{}, fmt.Errorf("rehash-work-per-cycle must be non-negative")
	}

	cfg.MaxMemoryPolicy = store.EvictionPolicy(policy)
	if err := cfg.MemoryConfig(store.RealClock{}).Validate(); err != nil {
		return Config{}, err
	}

	cfg.ReplicationRole = ReplicationRole(role)
	switch cfg.ReplicationRole {
	case ReplicationStandalone:
		if cfg.ReplicaOf != "" {
			return Config{}, fmt.Errorf("replicaof requires replication-role=replica")
		}
	case ReplicationPrimary:
		if cfg.ReplicationAddr == "" {
			return Config{}, fmt.Errorf("replication-addr is required for primary role")
		}
		if cfg.ReplicaOf != "" {
			return Config{}, fmt.Errorf("primary role cannot configure replicaof")
		}
	case ReplicationReplica:
		if cfg.ReplicaOf == "" {
			return Config{}, fmt.Errorf("replicaof is required for replica role")
		}
		if cfg.AppendOnly {
			return Config{}, fmt.Errorf("v0.8 replica role requires appendonly=false; replicated state is memory-only in this teaching version")
		}
		if cfg.MaxMemory > 0 {
			return Config{}, fmt.Errorf("v0.8 replica role requires maxmemory=0 so local eviction cannot diverge from primary policy")
		}
	default:
		return Config{}, fmt.Errorf("invalid replication-role %q", role)
	}
	if cfg.ReplicationBacklogBytes <= 0 {
		return Config{}, fmt.Errorf("repl-backlog-bytes must be positive")
	}
	if cfg.ReplicationSubscriberFrames <= 0 {
		return Config{}, fmt.Errorf("repl-subscriber-frames must be positive")
	}
	if cfg.ReplicationReconnectDelay <= 0 {
		return Config{}, fmt.Errorf("repl-reconnect-delay must be positive")
	}
	return cfg, nil
}
