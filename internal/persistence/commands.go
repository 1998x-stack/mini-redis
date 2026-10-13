package persistence

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/1998x-stack/mini-redis/internal/store"
)

// Commands converts a logical snapshot into the minimal command stream needed
// to reconstruct that state. Expiry is emitted as PEXPIREAT so restart time
// never extends a TTL.
func Commands(entries []store.SnapshotEntry) ([][]string, error) {
	ordered := append([]store.SnapshotEntry(nil), entries...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Key < ordered[j].Key })
	commands := make([][]string, 0, len(ordered)*2)

	for _, entry := range ordered {
		if entry.Key == "" {
			return nil, fmt.Errorf("persistence commands: empty key")
		}
		var cmd []string
		switch entry.Type {
		case store.ObjectString:
			cmd = []string{"SET", entry.Key, entry.StringValue}
		case store.ObjectList:
			if len(entry.ListValues) == 0 {
				return nil, fmt.Errorf("persistence commands %q: empty list", entry.Key)
			}
			cmd = append([]string{"RPUSH", entry.Key}, entry.ListValues...)
		case store.ObjectHash:
			if len(entry.HashValues) == 0 {
				return nil, fmt.Errorf("persistence commands %q: empty hash", entry.Key)
			}
			cmd = []string{"HSET", entry.Key}
			for _, pair := range entry.HashValues {
				cmd = append(cmd, pair.Field, pair.Value)
			}
		case store.ObjectSet:
			if len(entry.SetValues) == 0 {
				return nil, fmt.Errorf("persistence commands %q: empty set", entry.Key)
			}
			cmd = append([]string{"SADD", entry.Key}, entry.SetValues...)
		case store.ObjectZSet:
			if len(entry.ZSetValues) == 0 {
				return nil, fmt.Errorf("persistence commands %q: empty zset", entry.Key)
			}
			cmd = []string{"ZADD", entry.Key}
			for _, pair := range entry.ZSetValues {
				cmd = append(cmd, strconv.FormatFloat(pair.Score, 'g', -1, 64), pair.Member)
			}
		default:
			return nil, fmt.Errorf("persistence commands %q: unsupported type %d", entry.Key, entry.Type)
		}
		commands = append(commands, cmd)
		if entry.ExpireAtUnixMilli != 0 {
			commands = append(commands, []string{"PEXPIREAT", entry.Key, strconv.FormatInt(entry.ExpireAtUnixMilli, 10)})
		}
	}
	return commands, nil
}
