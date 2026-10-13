package cluster

import (
	"errors"
	"fmt"
	"strings"
)

var ErrCrossSlot = errors.New("CROSSSLOT Keys in request don't hash to the same slot")

type Session struct {
	asking bool
}

func (s *Session) GrantAsking() { s.asking = true }

func (s *Session) consumeAsking() bool {
	asking := s.asking
	s.asking = false
	return asking
}

func (s *Session) Asking() bool { return s.asking }

// CommandKeys describes the key positions in the current educational command
// subset. The bool reports whether routing is needed at all.
func CommandKeys(argv []string) ([]string, bool, error) {
	if len(argv) == 0 {
		return nil, false, errors.New("ERR empty command")
	}
	name := strings.ToUpper(argv[0])
	switch name {
	case "PING", "ECHO", "COMMAND", "DBSIZE", "KEYS", "FLUSHDB", "PUBLISH", "SUBSCRIBE", "UNSUBSCRIBE":
		return nil, false, nil
	case "OBJECT":
		if len(argv) >= 3 && strings.EqualFold(argv[1], "ENCODING") {
			return []string{argv[2]}, true, nil
		}
		return nil, false, nil
	case "DEL", "EXISTS", "WATCH":
		if len(argv) < 2 {
			return nil, false, nil
		}
		return append([]string(nil), argv[1:]...), true, nil
	case "SET", "GET", "INCR", "INCRBY", "EXPIRE", "PEXPIREAT", "TTL",
		"LPUSH", "RPUSH", "LPOP", "RPOP", "LLEN", "LRANGE",
		"HSET", "HGET", "HDEL", "HLEN", "HGETALL",
		"SADD", "SREM", "SISMEMBER", "SMEMBERS", "SCARD",
		"ZADD", "ZSCORE", "ZRANK", "ZRANGE", "ZREM":
		if len(argv) < 2 {
			return nil, false, nil
		}
		return []string{argv[1]}, true, nil
	default:
		return nil, false, nil
	}
}

func CommonSlot(keys []string) (int, error) {
	if len(keys) == 0 {
		return -1, nil
	}
	slot := HashSlot(keys[0])
	for _, key := range keys[1:] {
		if HashSlot(key) != slot {
			return -1, ErrCrossSlot
		}
	}
	return slot, nil
}

func FormatRedirect(decision RouteDecision) string {
	switch decision.Action {
	case RouteMoved:
		return fmt.Sprintf("MOVED %d %s", decision.Slot, decision.Addr)
	case RouteAsk:
		return fmt.Sprintf("ASK %d %s", decision.Slot, decision.Addr)
	case RouteClusterDown:
		return "CLUSTERDOWN Hash slot not served"
	default:
		return ""
	}
}
