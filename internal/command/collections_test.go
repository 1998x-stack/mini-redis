package command

import (
	"reflect"
	"testing"
	"time"

	"github.com/1998x-stack/mini-redis/internal/resp"
)

func TestCollectionCommandsAndEncodingTransitions(t *testing.T) {
	r, ctx := testRegistry()

	args := []string{"RPUSH", "list", "a", "b", "c", "d", "e", "f", "g", "h", "i"}
	if reply := r.Execute(ctx, args).Reply; reply.Int != 9 {
		t.Fatalf("RPUSH=%#v", reply)
	}
	if enc := r.Execute(ctx, []string{"OBJECT", "ENCODING", "list"}).Reply; enc.Str != "quicklist" {
		t.Fatalf("list encoding=%q", enc.Str)
	}
	rangeReply := r.Execute(ctx, []string{"LRANGE", "list", "-2", "-1"}).Reply
	if got := []string{rangeReply.Array[0].Str, rangeReply.Array[1].Str}; !reflect.DeepEqual(got, []string{"h", "i"}) {
		t.Fatalf("LRANGE=%v", got)
	}

	hargs := []string{"HSET", "hash"}
	for i := 0; i < 9; i++ {
		hargs = append(hargs, string(rune('a'+i)), string(rune('0'+i)))
	}
	if reply := r.Execute(ctx, hargs).Reply; reply.Int != 9 {
		t.Fatalf("HSET=%#v", reply)
	}
	if enc := r.Execute(ctx, []string{"OBJECT", "ENCODING", "hash"}).Reply; enc.Str != "hashtable" {
		t.Fatalf("hash encoding=%q", enc.Str)
	}

	r.Execute(ctx, []string{"SADD", "set", "1", "2", "3"})
	if enc := r.Execute(ctx, []string{"OBJECT", "ENCODING", "set"}).Reply; enc.Str != "intset" {
		t.Fatalf("set encoding=%q", enc.Str)
	}
	r.Execute(ctx, []string{"SADD", "set", "01"})
	if enc := r.Execute(ctx, []string{"OBJECT", "ENCODING", "set"}).Reply; enc.Str != "hashtable" {
		t.Fatalf("upgraded set encoding=%q", enc.Str)
	}

	zargs := []string{"ZADD", "zset"}
	for i := 1; i <= 9; i++ {
		zargs = append(zargs, string(rune('0'+i)), string(rune('a'+i-1)))
	}
	if reply := r.Execute(ctx, zargs).Reply; reply.Int != 9 {
		t.Fatalf("ZADD=%#v", reply)
	}
	if enc := r.Execute(ctx, []string{"OBJECT", "ENCODING", "zset"}).Reply; enc.Str != "skiplist" {
		t.Fatalf("zset encoding=%q", enc.Str)
	}
	if rank := r.Execute(ctx, []string{"ZRANK", "zset", "e"}).Reply; rank.Int != 4 {
		t.Fatalf("ZRANK=%#v", rank)
	}
}

func TestCollectionWrongTypeAndAOF(t *testing.T) {
	r, ctx := testRegistry()
	r.Execute(ctx, []string{"SET", "k", "value"})
	for _, argv := range [][]string{
		{"LPUSH", "k", "a"},
		{"HSET", "k", "f", "v"},
		{"SADD", "k", "m"},
		{"ZADD", "k", "1", "m"},
	} {
		reply := r.Execute(ctx, argv).Reply
		if reply.Type != resp.ErrorType || reply.Str != wrongTypeMessage {
			t.Fatalf("%v => %#v", argv, reply)
		}
	}

	exec := r.Execute(ctx, []string{"RPUSH", "aof-list", "x", "y"})
	commands := exec.AOFCommands(time.Unix(100, 0))
	if len(commands) != 1 || !reflect.DeepEqual(commands[0], []string{"RPUSH", "aof-list", "x", "y"}) {
		t.Fatalf("AOF commands=%#v", commands)
	}
}
