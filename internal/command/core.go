package command

import (
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/1998x-stack/mini-redis/internal/resp"
	"github.com/1998x-stack/mini-redis/internal/store"
)

func RegisterCore(r *Registry) {
	specs := []Spec{
		{Name: "PING", Arity: -1, Handler: ping, Summary: "health check / echo"},
		{Name: "ECHO", Arity: 2, Handler: echo, Summary: "echo a bulk string"},
		{Name: "SET", Arity: -3, Mutating: true, Handler: set, RewriteAOF: rewriteSetAOF, Summary: "set a string, optional EX/PX"},
		{Name: "GET", Arity: 2, Handler: get, Summary: "get a string"},
		{Name: "DEL", Arity: -2, Mutating: true, Handler: del, Summary: "delete one or more keys"},
		{Name: "EXISTS", Arity: -2, Handler: exists, Summary: "count existing keys"},
		{Name: "INCR", Arity: 2, Mutating: true, Handler: incr, Summary: "increment integer string"},
		{Name: "INCRBY", Arity: 3, Mutating: true, Handler: incrby, Summary: "increment by integer"},
		{Name: "EXPIRE", Arity: 3, Mutating: true, Handler: expire, RewriteAOF: rewriteExpireAOF, Summary: "set TTL in seconds"},
		{Name: "PEXPIREAT", Arity: 3, Mutating: true, Handler: pexpireat, Summary: "set absolute TTL in unix milliseconds"},
		{Name: "TTL", Arity: 2, Handler: ttl, Summary: "remaining TTL in seconds"},
		{Name: "KEYS", Arity: 2, Handler: keys, Summary: "glob-style key scan (teaching only)"},
		{Name: "DBSIZE", Arity: 1, Handler: dbsize, Summary: "number of live keys"},
		{Name: "FLUSHDB", Arity: 1, Mutating: true, Handler: flushdb, Summary: "remove all keys"},
		{Name: "OBJECT", Arity: -2, Handler: objectInfo, Summary: "inspect object encoding"},
		{Name: "COMMAND", Arity: -1, Handler: commandInfo, Summary: "minimal compatibility response"},
	}
	for _, spec := range specs {
		r.Register(spec)
	}
}

func ping(_ *Context, args []string) resp.Value {
	if len(args) == 0 {
		return resp.SimpleString("PONG")
	}
	if len(args) == 1 {
		return resp.BulkString(args[0])
	}
	return resp.Error("ERR wrong number of arguments for 'ping' command")
}

func echo(_ *Context, args []string) resp.Value { return resp.BulkString(args[0]) }

func set(ctx *Context, args []string) resp.Value {
	key, value := args[0], args[1]
	var ttl *time.Duration

	// v0.1 supports one optional expiration modifier: EX seconds or PX ms.
	if len(args) > 2 {
		if len(args) != 4 {
			return resp.Error("ERR syntax error")
		}
		amount, err := strconv.ParseInt(args[3], 10, 64)
		if err != nil || amount <= 0 {
			return resp.Error("ERR invalid expire time in 'set' command")
		}
		var d time.Duration
		switch strings.ToUpper(args[2]) {
		case "EX":
			if amount > int64(math.MaxInt64/time.Second) {
				return resp.Error("ERR invalid expire time in 'set' command")
			}
			d = time.Duration(amount) * time.Second
		case "PX":
			if amount > int64(math.MaxInt64/time.Millisecond) {
				return resp.Error("ERR invalid expire time in 'set' command")
			}
			d = time.Duration(amount) * time.Millisecond
		default:
			return resp.Error("ERR syntax error")
		}
		ttl = &d
	}

	ctx.Store.Set(key, store.NewString(value), ttl)
	return resp.SimpleString("OK")
}

func get(ctx *Context, args []string) resp.Value {
	obj, ok := ctx.Store.Get(args[0])
	if !ok {
		return resp.NullBulkString()
	}
	if obj.Type != store.ObjectString {
		return resp.Error("WRONGTYPE operation against a key holding the wrong kind of value")
	}
	value, ok := obj.StringValue()
	if !ok {
		return resp.Error("ERR invalid string object encoding")
	}
	return resp.BulkString(value)
}

func del(ctx *Context, args []string) resp.Value {
	return resp.Integer(int64(ctx.Store.Delete(args...)))
}

func exists(ctx *Context, args []string) resp.Value {
	return resp.Integer(int64(ctx.Store.Exists(args...)))
}

func incr(ctx *Context, args []string) resp.Value {
	return doIncrBy(ctx, args[0], 1)
}

func incrby(ctx *Context, args []string) resp.Value {
	delta, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return resp.Error("ERR value is not an integer or out of range")
	}
	return doIncrBy(ctx, args[0], delta)
}

func doIncrBy(ctx *Context, key string, delta int64) resp.Value {
	value, err := ctx.Store.IncrBy(key, delta)
	if err != nil {
		return resp.Error("ERR value is not an integer or out of range")
	}
	return resp.Integer(value)
}

func expire(ctx *Context, args []string) resp.Value {
	seconds, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return resp.Error("ERR value is not an integer or out of range")
	}
	if seconds > int64(math.MaxInt64/time.Second) || seconds < int64(math.MinInt64/time.Second) {
		return resp.Error("ERR invalid expire time")
	}
	if ctx.Store.Expire(args[0], time.Duration(seconds)*time.Second) {
		return resp.Integer(1)
	}
	return resp.Integer(0)
}

func pexpireat(ctx *Context, args []string) resp.Value {
	millis, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return resp.Error("ERR value is not an integer or out of range")
	}
	if ctx.Store.ExpireAt(args[0], time.UnixMilli(millis)) {
		return resp.Integer(1)
	}
	return resp.Integer(0)
}

func rewriteSetAOF(argv []string, now time.Time) [][]string {
	if len(argv) == 3 {
		return [][]string{append([]string(nil), argv...)}
	}
	if len(argv) != 5 {
		return [][]string{append([]string(nil), argv...)}
	}
	amount, err := strconv.ParseInt(argv[4], 10, 64)
	if err != nil {
		return [][]string{append([]string(nil), argv...)}
	}
	var deadline time.Time
	switch strings.ToUpper(argv[3]) {
	case "EX":
		deadline = now.Add(time.Duration(amount) * time.Second)
	case "PX":
		deadline = now.Add(time.Duration(amount) * time.Millisecond)
	default:
		return [][]string{append([]string(nil), argv...)}
	}
	return [][]string{
		{"SET", argv[1], argv[2]},
		{"PEXPIREAT", argv[1], strconv.FormatInt(deadline.UnixMilli(), 10)},
	}
}

func rewriteExpireAOF(argv []string, now time.Time) [][]string {
	seconds, err := strconv.ParseInt(argv[2], 10, 64)
	if err != nil {
		return [][]string{append([]string(nil), argv...)}
	}
	deadline := now.Add(time.Duration(seconds) * time.Second)
	return [][]string{{"PEXPIREAT", argv[1], strconv.FormatInt(deadline.UnixMilli(), 10)}}
}

func ttl(ctx *Context, args []string) resp.Value {
	result := ctx.Store.TTL(args[0])
	if !result.Exists {
		return resp.Integer(-2)
	}
	if !result.HasExpiry {
		return resp.Integer(-1)
	}
	// Redis TTL is integer seconds. Ceiling avoids reporting 0 immediately
	// after EXPIRE key 1 due solely to sub-second processing time.
	seconds := int64((result.Remaining + time.Second - 1) / time.Second)
	if seconds < 0 {
		seconds = 0
	}
	return resp.Integer(seconds)
}

func keys(ctx *Context, args []string) resp.Value {
	names, err := ctx.Store.Keys(args[0])
	if err != nil {
		return resp.Error("ERR invalid pattern")
	}
	out := make([]resp.Value, 0, len(names))
	for _, name := range names {
		out = append(out, resp.BulkString(name))
	}
	return resp.Array(out)
}

func dbsize(ctx *Context, _ []string) resp.Value { return resp.Integer(int64(ctx.Store.Len())) }

func flushdb(ctx *Context, _ []string) resp.Value {
	ctx.Store.Flush()
	return resp.SimpleString("OK")
}

func objectInfo(ctx *Context, args []string) resp.Value {
	if len(args) != 2 || !strings.EqualFold(args[0], "ENCODING") {
		return resp.Error("ERR syntax error")
	}
	obj, ok := ctx.Store.Get(args[1])
	if !ok {
		return resp.NullBulkString()
	}
	return resp.BulkString(obj.Encoding.String())
}

func commandInfo(_ *Context, _ []string) resp.Value {
	// redis-cli can issue COMMAND during capability discovery. Returning an
	// empty array is enough for v0.1 interoperability without pretending to
	// implement Redis' full command metadata schema.
	return resp.Array(nil)
}
