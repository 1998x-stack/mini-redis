package config

import (
	"flag"
	"fmt"
	"time"

	"github.com/1998x-stack/mini-redis/internal/aof"
)

type Config struct {
	ShowVersion        bool
	Addr               string
	AppendOnly         bool
	AppendFilename     string
	AppendFsync        aof.FsyncPolicy
	ActiveExpireEvery  time.Duration
	ActiveExpireSample int
	IdleTimeout        time.Duration
}

func Default() Config {
	return Config{
		Addr:               "127.0.0.1:6379",
		AppendOnly:         true,
		AppendFilename:     "appendonly.aof",
		AppendFsync:        aof.FsyncEverySec,
		ActiveExpireEvery:  100 * time.Millisecond,
		ActiveExpireSample: 64,
		IdleTimeout:        0,
	}
}

func ParseFlags() (Config, error) {
	cfg := Default()
	fsync := string(cfg.AppendFsync)

	flag.BoolVar(&cfg.ShowVersion, "version", false, "print version and exit")
	flag.StringVar(&cfg.Addr, "addr", cfg.Addr, "listen address")
	flag.BoolVar(&cfg.AppendOnly, "appendonly", cfg.AppendOnly, "enable AOF persistence")
	flag.StringVar(&cfg.AppendFilename, "appendfilename", cfg.AppendFilename, "AOF file path")
	flag.StringVar(&fsync, "appendfsync", fsync, "AOF fsync policy: always|everysec|no")
	flag.DurationVar(&cfg.ActiveExpireEvery, "active-expire-every", cfg.ActiveExpireEvery, "active expiration cycle interval")
	flag.IntVar(&cfg.ActiveExpireSample, "active-expire-sample", cfg.ActiveExpireSample, "max TTL entries checked per cycle; <=0 full scan")
	flag.DurationVar(&cfg.IdleTimeout, "idle-timeout", cfg.IdleTimeout, "connection idle timeout; 0 disables")
	flag.Parse()

	cfg.AppendFsync = aof.FsyncPolicy(fsync)
	switch cfg.AppendFsync {
	case aof.FsyncAlways, aof.FsyncEverySec, aof.FsyncNo:
	default:
		return Config{}, fmt.Errorf("invalid appendfsync %q", fsync)
	}
	if cfg.ActiveExpireEvery <= 0 {
		return Config{}, fmt.Errorf("active-expire-every must be positive")
	}
	return cfg, nil
}
