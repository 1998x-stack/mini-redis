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
}

func Default() Config {
	memory := store.DefaultMemoryConfig()
	return Config{
		Addr:                    "127.0.0.1:6379",
		NetworkEngine:           NetworkAuto,
		ClientInputBufferLimit:  64 << 20, // aligned with the RESP bulk-string teaching cap
		ClientOutputBufferLimit: 8 << 20,
		AppendOnly:              true,
		AppendFilename:          "appendonly.aof",
		AppendFsync:             aof.FsyncEverySec,
		RDBFilename:             "dump.mrdb",
		ActiveExpireEvery:       100 * time.Millisecond,
		ActiveExpireSample:      64,
		RehashWorkPerCycle:      64,
		IdleTimeout:             0,
		MaxMemory:               memory.MaxBytes,
		MaxMemoryPolicy:         memory.Policy,
		MaxMemorySamples:        memory.Samples,
		LFUDecay:                memory.LFUDecay,
		EvictionSeed:            memory.RandomSeed,
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
	return cfg, nil
}
