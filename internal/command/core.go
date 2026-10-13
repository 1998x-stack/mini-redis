package command

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/1998x-stack/mini-redis/internal/resp"
	"github.com/1998x-stack/mini-redis/internal/store"
)

const wrongTypeMessage = "WRONGTYPE operation against a key holding the wrong kind of value"

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

		{Name: "LPUSH", Arity: -3, Mutating: true, Handler: lpush, Summary: "prepend list values"},
		{Name: "RPUSH", Arity: -3, Mutating: true, Handler: rpush, Summary: "append list values"},
		{Name: "LPOP", Arity: 2, Mutating: true, Handler: lpop, Summary: "pop list head"},
		{Name: "RPOP", Arity: 2, Mutating: true, Handler: rpop, Summary: "pop list tail"},
		{Name: "LLEN", Arity: 2, Handler: llen, Summary: "list length"},
		{Name: "LRANGE", Arity: 4, Handler: lrange, Summary: "inclusive list range"},

		{Name: "HSET", Arity: -4, Mutating: true, Handler: hset, Summary: "set hash fields"},
		{Name: "HGET", Arity: 3, Handler: hget, Summary: "get hash field"},
		{Name: "HDEL", Arity: -3, Mutating: true, Handler: hdel, Summary: "delete hash fields"},
		{Name: "HLEN", Arity: 2, Handler: hlen, Summary: "hash field count"},
		{Name: "HGETALL", Arity: 2, Handler: hgetall, Summary: "all hash fields and values"},

		{Name: "SADD", Arity: -3, Mutating: true, Handler: sadd, Summary: "add set members"},
		{Name: "SREM", Arity: -3, Mutating: true, Handler: srem, Summary: "remove set members"},
		{Name: "SISMEMBER", Arity: 3, Handler: sismember, Summary: "set membership"},
		{Name: "SMEMBERS", Arity: 2, Handler: smembers, Summary: "all set members"},
		{Name: "SCARD", Arity: 2, Handler: scard, Summary: "set cardinality"},

		{Name: "ZADD", Arity: -4, Mutating: true, Handler: zadd, Summary: "add/update sorted-set members"},
		{Name: "ZSCORE", Arity: 3, Handler: zscore, Summary: "sorted-set score"},
		{Name: "ZRANK", Arity: 3, Handler: zrank, Summary: "zero-based sorted-set rank"},
		{Name: "ZRANGE", Arity: 4, Handler: zrange, Summary: "sorted-set rank range"},
		{Name: "ZREM", Arity: -3, Mutating: true, Handler: zrem, Summary: "remove sorted-set members"},

		{Name: "COMMAND", Arity: -1, Handler: commandInfo, Summary: "minimal compatibility response"},
	}
	for _, spec := range specs {
		r.Register(spec)
	}
}

func wrongType(err error) resp.Value {
	if errors.Is(err, store.ErrWrongType) {
		return resp.Error(wrongTypeMessage)
	}
	return resp.Error("ERR internal data error")
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
		return resp.Error(wrongTypeMessage)
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

func incr(ctx *Context, args []string) resp.Value { return doIncrBy(ctx, args[0], 1) }

func incrby(ctx *Context, args []string) resp.Value {
	delta, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return resp.Error("ERR value is not an integer or out of range")
	}
	return doIncrBy(ctx, args[0], delta)
}

func doIncrBy(ctx *Context, key string, delta int64) resp.Value {
	value, err := ctx.Store.IncrBy(key, delta)
	if errors.Is(err, store.ErrWrongType) {
		return resp.Error(wrongTypeMessage)
	}
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
	return bulkArray(names)
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

func lpush(ctx *Context, args []string) resp.Value { return listPush(ctx, args, true) }
func rpush(ctx *Context, args []string) resp.Value { return listPush(ctx, args, false) }

func listPush(ctx *Context, args []string, left bool) resp.Value {
	n, err := ctx.Store.ListPush(args[0], left, args[1:])
	if err != nil {
		return wrongType(err)
	}
	return resp.Integer(int64(n))
}

func lpop(ctx *Context, args []string) resp.Value { return listPop(ctx, args, true) }
func rpop(ctx *Context, args []string) resp.Value { return listPop(ctx, args, false) }

func listPop(ctx *Context, args []string, left bool) resp.Value {
	value, ok, err := ctx.Store.ListPop(args[0], left)
	if err != nil {
		return wrongType(err)
	}
	if !ok {
		return resp.NullBulkString()
	}
	return resp.BulkString(value)
}

func llen(ctx *Context, args []string) resp.Value {
	n, err := ctx.Store.ListLen(args[0])
	if err != nil {
		return wrongType(err)
	}
	return resp.Integer(int64(n))
}

func lrange(ctx *Context, args []string) resp.Value {
	start, err := strconv.Atoi(args[1])
	if err != nil {
		return resp.Error("ERR value is not an integer or out of range")
	}
	stop, err := strconv.Atoi(args[2])
	if err != nil {
		return resp.Error("ERR value is not an integer or out of range")
	}
	values, err := ctx.Store.ListRange(args[0], start, stop)
	if err != nil {
		return wrongType(err)
	}
	return bulkArray(values)
}

func hset(ctx *Context, args []string) resp.Value {
	if len(args) < 3 || len(args)%2 == 0 {
		return resp.Error("ERR wrong number of arguments for 'hset' command")
	}
	pairs := make([]store.FieldValue, 0, (len(args)-1)/2)
	for i := 1; i < len(args); i += 2 {
		pairs = append(pairs, store.FieldValue{Field: args[i], Value: args[i+1]})
	}
	n, err := ctx.Store.HashSet(args[0], pairs)
	if err != nil {
		return wrongType(err)
	}
	return resp.Integer(int64(n))
}

func hget(ctx *Context, args []string) resp.Value {
	value, ok, err := ctx.Store.HashGet(args[0], args[1])
	if err != nil {
		return wrongType(err)
	}
	if !ok {
		return resp.NullBulkString()
	}
	return resp.BulkString(value)
}

func hdel(ctx *Context, args []string) resp.Value {
	n, err := ctx.Store.HashDelete(args[0], args[1:]...)
	if err != nil {
		return wrongType(err)
	}
	return resp.Integer(int64(n))
}

func hlen(ctx *Context, args []string) resp.Value {
	n, err := ctx.Store.HashLen(args[0])
	if err != nil {
		return wrongType(err)
	}
	return resp.Integer(int64(n))
}

func hgetall(ctx *Context, args []string) resp.Value {
	pairs, err := ctx.Store.HashGetAll(args[0])
	if err != nil {
		return wrongType(err)
	}
	out := make([]resp.Value, 0, len(pairs)*2)
	for _, pair := range pairs {
		out = append(out, resp.BulkString(pair.Field), resp.BulkString(pair.Value))
	}
	return resp.Array(out)
}

func sadd(ctx *Context, args []string) resp.Value {
	n, err := ctx.Store.SetAdd(args[0], args[1:]...)
	if err != nil {
		return wrongType(err)
	}
	return resp.Integer(int64(n))
}

func srem(ctx *Context, args []string) resp.Value {
	n, err := ctx.Store.SetRemove(args[0], args[1:]...)
	if err != nil {
		return wrongType(err)
	}
	return resp.Integer(int64(n))
}

func sismember(ctx *Context, args []string) resp.Value {
	ok, err := ctx.Store.SetIsMember(args[0], args[1])
	if err != nil {
		return wrongType(err)
	}
	if ok {
		return resp.Integer(1)
	}
	return resp.Integer(0)
}

func smembers(ctx *Context, args []string) resp.Value {
	values, err := ctx.Store.SetMembers(args[0])
	if err != nil {
		return wrongType(err)
	}
	return bulkArray(values)
}

func scard(ctx *Context, args []string) resp.Value {
	n, err := ctx.Store.SetCard(args[0])
	if err != nil {
		return wrongType(err)
	}
	return resp.Integer(int64(n))
}

func zadd(ctx *Context, args []string) resp.Value {
	if len(args) < 3 || len(args)%2 == 0 {
		return resp.Error("ERR wrong number of arguments for 'zadd' command")
	}
	pairs := make([]store.ScoredMember, 0, (len(args)-1)/2)
	for i := 1; i < len(args); i += 2 {
		score, err := strconv.ParseFloat(args[i], 64)
		if err != nil || math.IsNaN(score) {
			return resp.Error("ERR value is not a valid float")
		}
		pairs = append(pairs, store.ScoredMember{Member: args[i+1], Score: score})
	}
	n, err := ctx.Store.ZAdd(args[0], pairs)
	if err != nil {
		return wrongType(err)
	}
	return resp.Integer(int64(n))
}

func zscore(ctx *Context, args []string) resp.Value {
	score, ok, err := ctx.Store.ZScore(args[0], args[1])
	if err != nil {
		return wrongType(err)
	}
	if !ok {
		return resp.NullBulkString()
	}
	return resp.BulkString(strconv.FormatFloat(score, 'g', -1, 64))
}

func zrank(ctx *Context, args []string) resp.Value {
	rank, ok, err := ctx.Store.ZRank(args[0], args[1])
	if err != nil {
		return wrongType(err)
	}
	if !ok {
		return resp.NullBulkString()
	}
	return resp.Integer(int64(rank))
}

func zrange(ctx *Context, args []string) resp.Value {
	start, err := strconv.Atoi(args[1])
	if err != nil {
		return resp.Error("ERR value is not an integer or out of range")
	}
	stop, err := strconv.Atoi(args[2])
	if err != nil {
		return resp.Error("ERR value is not an integer or out of range")
	}
	pairs, err := ctx.Store.ZRange(args[0], start, stop)
	if err != nil {
		return wrongType(err)
	}
	out := make([]resp.Value, 0, len(pairs))
	for _, pair := range pairs {
		out = append(out, resp.BulkString(pair.Member))
	}
	return resp.Array(out)
}

func zrem(ctx *Context, args []string) resp.Value {
	n, err := ctx.Store.ZRemove(args[0], args[1:]...)
	if err != nil {
		return wrongType(err)
	}
	return resp.Integer(int64(n))
}

func bulkArray(values []string) resp.Value {
	out := make([]resp.Value, 0, len(values))
	for _, value := range values {
		out = append(out, resp.BulkString(value))
	}
	return resp.Array(out)
}

func commandInfo(_ *Context, _ []string) resp.Value {
	return resp.Array(nil)
}
