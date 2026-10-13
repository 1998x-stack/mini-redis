package command

import (
	"fmt"
	"strings"
	"time"

	"github.com/1998x-stack/mini-redis/internal/resp"
	"github.com/1998x-stack/mini-redis/internal/store"
)

type Handler func(ctx *Context, args []string) resp.Value

// AOFRewrite converts a successful mutating command into the command sequence
// that should be persisted. Most commands return themselves. Relative expiry
// commands rewrite to absolute deadlines so downtime does not extend TTLs.
type AOFRewrite func(argv []string, now time.Time) [][]string

type Spec struct {
	Name       string
	Arity      int // >0 exact argc including command; <0 minimum argc.
	Mutating   bool
	Handler    Handler
	RewriteAOF AOFRewrite
	Summary    string
}

type Context struct {
	Store store.Store
}

type Registry struct {
	commands map[string]Spec
}

type Execution struct {
	Reply    resp.Value
	Spec     Spec
	Args     []string
	Found    bool
	Runnable bool
}

func NewRegistry() *Registry {
	return &Registry{commands: make(map[string]Spec)}
}

func (r *Registry) Register(spec Spec) {
	name := strings.ToUpper(spec.Name)
	if name == "" || spec.Handler == nil {
		panic("invalid command spec")
	}
	if _, exists := r.commands[name]; exists {
		panic("duplicate command: " + name)
	}
	spec.Name = name
	r.commands[name] = spec
}

// Lookup exposes immutable command metadata before execution. v0.6 uses this
// to reason about memory pressure without executing a handler first.
func (r *Registry) Lookup(name string) (Spec, bool) {
	spec, ok := r.commands[strings.ToUpper(name)]
	return spec, ok
}

// MayGrowMemory identifies the current mutating command subset that can create
// a key/value, enlarge a value, or add expiry metadata. Removal-only commands
// remain usable under noeviction even if a process starts above maxmemory.
func (r *Registry) MayGrowMemory(name string) bool {
	spec, ok := r.Lookup(name)
	if !ok || !spec.Mutating {
		return false
	}
	switch spec.Name {
	case "SET", "INCR", "INCRBY", "EXPIRE", "PEXPIREAT",
		"LPUSH", "RPUSH", "HSET", "SADD", "ZADD":
		return true
	default:
		return false
	}
}

func (r *Registry) Execute(ctx *Context, argv []string) Execution {
	if len(argv) == 0 {
		return Execution{Reply: resp.Error("ERR empty command")}
	}

	name := strings.ToUpper(argv[0])
	spec, ok := r.commands[name]
	if !ok {
		return Execution{
			Reply: resp.Error(fmt.Sprintf("ERR unknown command '%s'", strings.ToLower(argv[0]))),
			Args:  argv,
		}
	}

	argc := len(argv)
	valid := (spec.Arity > 0 && argc == spec.Arity) || (spec.Arity < 0 && argc >= -spec.Arity)
	if !valid {
		return Execution{
			Reply: resp.Error(fmt.Sprintf("ERR wrong number of arguments for '%s' command", strings.ToLower(spec.Name))),
			Spec:  spec,
			Args:  argv,
			Found: true,
		}
	}

	reply := spec.Handler(ctx, argv[1:])
	return Execution{Reply: reply, Spec: spec, Args: argv, Found: true, Runnable: true}
}

func (e Execution) AOFCommands(now time.Time) [][]string {
	if !e.Runnable || !e.Spec.Mutating || e.Reply.IsError() {
		return nil
	}
	if e.Spec.RewriteAOF != nil {
		return e.Spec.RewriteAOF(e.Args, now)
	}
	return [][]string{append([]string(nil), e.Args...)}
}

func (r *Registry) Specs() []Spec {
	out := make([]Spec, 0, len(r.commands))
	for _, spec := range r.commands {
		out = append(out, spec)
	}
	return out
}
