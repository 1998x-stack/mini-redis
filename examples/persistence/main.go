package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/1998x-stack/mini-redis/internal/aof"
	"github.com/1998x-stack/mini-redis/internal/persistence"
	"github.com/1998x-stack/mini-redis/internal/rdb"
	"github.com/1998x-stack/mini-redis/internal/store"
	"github.com/1998x-stack/mini-redis/internal/store/versionedmemory"
)

func main() {
	dir, err := os.MkdirTemp("", "mini-redis-v05-")
	must(err)
	defer os.RemoveAll(dir)

	memory := versionedmemory.New(store.RealClock{})
	memory.Set("name", store.NewString("mini-redis"), nil)
	_, _ = memory.ListPush("jobs", false, []string{"parse", "execute", "persist"})
	_, _ = memory.HashSet("user", []store.FieldValue{{Field: "name", Value: "alice"}, {Field: "role", Value: "learner"}})
	_, _ = memory.SetAdd("ids", "1", "2", "3")
	_, _ = memory.ZAdd("rank", []store.ScoredMember{{Member: "alice", Score: 10}, {Member: "bob", Score: 20}})

	entries, err := memory.Snapshot()
	must(err)
	rdbPath := filepath.Join(dir, "dump.mrdb")
	must(rdb.SaveFile(rdbPath, entries))
	loaded, err := rdb.LoadFile(rdbPath)
	must(err)
	restored := versionedmemory.New(store.RealClock{})
	must(restored.RestoreSnapshot(loaded))
	fmt.Printf("RDB: keys=%d file=%s\n", restored.Len(), filepath.Base(rdbPath))

	aofPath := filepath.Join(dir, "appendonly.aof")
	log, err := aof.Open(aofPath, aof.FsyncAlways)
	must(err)
	defer log.Close()
	for _, value := range []string{"old-1", "old-2", "mini-redis"} {
		must(log.Append([]string{"SET", "name", value}))
	}
	commands, err := persistence.Commands(entries)
	must(err)
	must(log.BeginRewrite())
	// This mutation happens after the snapshot boundary. AOF rewrite must keep it.
	must(log.Append([]string{"SET", "after-snapshot", "kept"}))
	must(log.FinishRewrite(commands))

	count := 0
	must(log.Replay(func(argv []string) error {
		count++
		return nil
	}))
	fmt.Printf("AOF rewrite: compact-base=%d replay-commands=%d buffered-mutation=kept\n", len(commands), count)
	fmt.Println("publication: temp -> fsync(file) -> rename -> fsync(dir)")
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
