package server

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/1998x-stack/mini-redis/internal/resp"
)

type watchToken struct {
	epoch   uint64
	version uint64
	exists  bool
}

type keyVersionTable struct {
	mu      sync.RWMutex
	seq     uint64
	epoch   uint64
	version map[string]uint64
}

func newKeyVersionTable() *keyVersionTable {
	return &keyVersionTable{version: make(map[string]uint64)}
}

func (v *keyVersionTable) token(key string, exists bool) watchToken {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return watchToken{epoch: v.epoch, version: v.version[key], exists: exists}
}

func (v *keyVersionTable) changed(key string, token watchToken, exists bool) bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return token.epoch != v.epoch || token.version != v.version[key] || token.exists != exists
}

func (v *keyVersionTable) bump(keys ...string) {
	if len(keys) == 0 {
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.seq++
	for _, key := range keys {
		v.version[key] = v.seq
	}
}

func (v *keyVersionTable) bumpAll() {
	v.mu.Lock()
	v.seq++
	v.epoch++
	v.mu.Unlock()
}

type clientSession struct {
	id uint64

	mu            sync.Mutex
	inMulti       bool
	dirty         bool
	queue         [][]string
	watched       map[string]watchToken
	subscriptions map[string]struct{}
	push          func(resp.Value) error
}

func (s *Server) newClientSession(push func(resp.Value) error) *clientSession {
	return &clientSession{
		id:            s.sessionSeq.Add(1),
		watched:       make(map[string]watchToken),
		subscriptions: make(map[string]struct{}),
		push:          push,
	}
}

func (s *Server) closeClientSession(session *clientSession) {
	if session == nil {
		return
	}
	s.hub.unsubscribeAll(session)
}

func (c *clientSession) subscriptionCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.subscriptions)
}

func (c *clientSession) subscribed() bool { return c.subscriptionCount() > 0 }

func (c *clientSession) resetTransaction(clearWatches bool) {
	c.mu.Lock()
	c.inMulti = false
	c.dirty = false
	c.queue = nil
	if clearWatches {
		c.watched = make(map[string]watchToken)
	}
	c.mu.Unlock()
}

type pubSubHub struct {
	mu       sync.Mutex
	channels map[string]map[*clientSession]struct{}
}

func newPubSubHub() *pubSubHub {
	return &pubSubHub{channels: make(map[string]map[*clientSession]struct{})}
}

func (h *pubSubHub) subscribe(session *clientSession, channel string) int {
	h.mu.Lock()
	subs := h.channels[channel]
	if subs == nil {
		subs = make(map[*clientSession]struct{})
		h.channels[channel] = subs
	}
	subs[session] = struct{}{}
	h.mu.Unlock()

	session.mu.Lock()
	session.subscriptions[channel] = struct{}{}
	count := len(session.subscriptions)
	session.mu.Unlock()
	return count
}

func (h *pubSubHub) unsubscribe(session *clientSession, channel string) int {
	h.mu.Lock()
	if subs := h.channels[channel]; subs != nil {
		delete(subs, session)
		if len(subs) == 0 {
			delete(h.channels, channel)
		}
	}
	h.mu.Unlock()

	session.mu.Lock()
	delete(session.subscriptions, channel)
	count := len(session.subscriptions)
	session.mu.Unlock()
	return count
}

func (h *pubSubHub) unsubscribeAll(session *clientSession) []string {
	session.mu.Lock()
	channels := make([]string, 0, len(session.subscriptions))
	for channel := range session.subscriptions {
		channels = append(channels, channel)
	}
	session.mu.Unlock()
	sort.Strings(channels)
	for _, channel := range channels {
		h.unsubscribe(session, channel)
	}
	return channels
}

func (h *pubSubHub) publish(channel, payload string) int {
	h.mu.Lock()
	subs := h.channels[channel]
	targets := make([]*clientSession, 0, len(subs))
	for session := range subs {
		targets = append(targets, session)
	}
	h.mu.Unlock()

	message := resp.Array([]resp.Value{
		resp.BulkString("message"),
		resp.BulkString(channel),
		resp.BulkString(payload),
	})
	for _, session := range targets {
		if session.push != nil {
			_ = session.push(message)
		}
	}
	return len(targets)
}

func pubSubCountReply(kind, channel string, count int) resp.Value {
	return resp.Array([]resp.Value{
		resp.BulkString(kind),
		resp.BulkString(channel),
		resp.Integer(int64(count)),
	})
}

func (s *Server) executeClientCommand(session *clientSession, argv []string) []resp.Value {
	if len(argv) == 0 {
		return []resp.Value{resp.Error("ERR empty command")}
	}
	name := strings.ToUpper(argv[0])

	if session.subscribed() {
		switch name {
		case "SUBSCRIBE":
			return s.subscribeCommand(session, argv)
		case "UNSUBSCRIBE":
			return s.unsubscribeCommand(session, argv)
		case "PING":
			if len(argv) > 2 {
				return []resp.Value{resp.Error("ERR wrong number of arguments for 'ping' command")}
			}
			payload := ""
			if len(argv) == 2 {
				payload = argv[1]
			}
			return []resp.Value{resp.Array([]resp.Value{resp.BulkString("pong"), resp.BulkString(payload)})}
		default:
			return []resp.Value{resp.Error("ERR only SUBSCRIBE, UNSUBSCRIBE and PING are allowed in subscribed mode")}
		}
	}

	switch name {
	case "MULTI":
		if len(argv) != 1 {
			return []resp.Value{resp.Error("ERR wrong number of arguments for 'multi' command")}
		}
		session.mu.Lock()
		defer session.mu.Unlock()
		if session.inMulti {
			return []resp.Value{resp.Error("ERR MULTI calls can not be nested")}
		}
		session.inMulti = true
		session.dirty = false
		session.queue = nil
		return []resp.Value{resp.SimpleString("OK")}
	case "EXEC":
		return []resp.Value{s.execTransaction(session)}
	case "DISCARD":
		if len(argv) != 1 {
			return []resp.Value{resp.Error("ERR wrong number of arguments for 'discard' command")}
		}
		session.mu.Lock()
		inMulti := session.inMulti
		session.mu.Unlock()
		if !inMulti {
			return []resp.Value{resp.Error("ERR DISCARD without MULTI")}
		}
		session.resetTransaction(true)
		return []resp.Value{resp.SimpleString("OK")}
	case "WATCH":
		return []resp.Value{s.watchCommand(session, argv)}
	case "UNWATCH":
		return []resp.Value{s.unwatchCommand(session, argv)}
	case "SUBSCRIBE":
		return s.subscribeCommand(session, argv)
	case "UNSUBSCRIBE":
		return s.unsubscribeCommand(session, argv)
	case "PUBLISH":
		session.mu.Lock()
		inMulti := session.inMulti
		if inMulti {
			session.dirty = true
		}
		session.mu.Unlock()
		if inMulti {
			return []resp.Value{resp.Error("ERR PUBLISH is not queued inside MULTI in mini-redis v0.7")}
		}
		if len(argv) != 3 {
			return []resp.Value{resp.Error("ERR wrong number of arguments for 'publish' command")}
		}
		return []resp.Value{resp.Integer(int64(s.hub.publish(argv[1], argv[2])))}
	}

	session.mu.Lock()
	if session.inMulti {
		if _, err := s.registry.Validate(argv); err != nil {
			session.dirty = true
			session.mu.Unlock()
			return []resp.Value{resp.Error(strings.TrimPrefix(err.Error(), "ERR "))}
		}
		queued := append([]string(nil), argv...)
		session.queue = append(session.queue, queued)
		session.mu.Unlock()
		return []resp.Value{resp.SimpleString("QUEUED")}
	}
	session.mu.Unlock()
	return []resp.Value{s.executeCommand(argv)}
}

func (s *Server) watchCommand(session *clientSession, argv []string) resp.Value {
	if len(argv) < 2 {
		return resp.Error("ERR wrong number of arguments for 'watch' command")
	}
	session.mu.Lock()
	if session.inMulti {
		session.mu.Unlock()
		return resp.Error("ERR WATCH inside MULTI is not allowed")
	}
	session.mu.Unlock()

	s.persistenceMu.Lock()
	tokens := make(map[string]watchToken, len(argv)-1)
	for _, key := range argv[1:] {
		exists := s.store.Exists(key) > 0
		tokens[key] = s.versions.token(key, exists)
	}
	s.persistenceMu.Unlock()

	session.mu.Lock()
	for key, token := range tokens {
		session.watched[key] = token
	}
	session.mu.Unlock()
	return resp.SimpleString("OK")
}

func (s *Server) unwatchCommand(session *clientSession, argv []string) resp.Value {
	if len(argv) != 1 {
		return resp.Error("ERR wrong number of arguments for 'unwatch' command")
	}
	session.mu.Lock()
	if session.inMulti {
		session.mu.Unlock()
		return resp.Error("ERR UNWATCH inside MULTI is not allowed")
	}
	session.watched = make(map[string]watchToken)
	session.mu.Unlock()
	return resp.SimpleString("OK")
}

func (s *Server) execTransaction(session *clientSession) resp.Value {
	session.mu.Lock()
	if !session.inMulti {
		session.mu.Unlock()
		return resp.Error("ERR EXEC without MULTI")
	}
	dirty := session.dirty
	queue := make([][]string, len(session.queue))
	for i := range session.queue {
		queue[i] = append([]string(nil), session.queue[i]...)
	}
	watched := make(map[string]watchToken, len(session.watched))
	for key, token := range session.watched {
		watched[key] = token
	}
	session.mu.Unlock()

	if dirty {
		session.resetTransaction(true)
		return resp.Error("EXECABORT Transaction discarded because of previous errors.")
	}

	s.persistenceMu.Lock()
	for key, token := range watched {
		exists := s.store.Exists(key) > 0
		if s.versions.changed(key, token, exists) {
			s.persistenceMu.Unlock()
			session.resetTransaction(true)
			return resp.NullArray()
		}
	}

	replies := make([]resp.Value, 0, len(queue))
	for _, command := range queue {
		replies = append(replies, s.executeCommandLocked(command))
	}
	s.persistenceMu.Unlock()
	session.resetTransaction(true)
	return resp.Array(replies)
}

func (s *Server) subscribeCommand(session *clientSession, argv []string) []resp.Value {
	if len(argv) < 2 {
		return []resp.Value{resp.Error("ERR wrong number of arguments for 'subscribe' command")}
	}
	session.mu.Lock()
	inMulti := session.inMulti
	if inMulti {
		session.dirty = true
	}
	session.mu.Unlock()
	if inMulti {
		return []resp.Value{resp.Error("ERR SUBSCRIBE is not allowed inside MULTI")}
	}

	replies := make([]resp.Value, 0, len(argv)-1)
	for _, channel := range argv[1:] {
		count := s.hub.subscribe(session, channel)
		replies = append(replies, pubSubCountReply("subscribe", channel, count))
	}
	return replies
}

func (s *Server) unsubscribeCommand(session *clientSession, argv []string) []resp.Value {
	session.mu.Lock()
	inMulti := session.inMulti
	if inMulti {
		session.dirty = true
	}
	session.mu.Unlock()
	if inMulti {
		return []resp.Value{resp.Error("ERR UNSUBSCRIBE is not allowed inside MULTI")}
	}

	channels := argv[1:]
	if len(channels) == 0 {
		session.mu.Lock()
		channels = make([]string, 0, len(session.subscriptions))
		for channel := range session.subscriptions {
			channels = append(channels, channel)
		}
		session.mu.Unlock()
		sort.Strings(channels)
		if len(channels) == 0 {
			return []resp.Value{pubSubCountReply("unsubscribe", "", 0)}
		}
	}

	replies := make([]resp.Value, 0, len(channels))
	for _, channel := range channels {
		count := s.hub.unsubscribe(session, channel)
		replies = append(replies, pubSubCountReply("unsubscribe", channel, count))
	}
	return replies
}

func (s *Server) noteAcceptedMutation(argv []string, evicted []string) {
	keys, all := commandWriteKeys(argv)
	if all {
		s.versions.bumpAll()
	} else {
		s.versions.bump(keys...)
	}
	if len(evicted) > 0 {
		s.versions.bump(evicted...)
	}
}

func commandWriteKeys(argv []string) ([]string, bool) {
	if len(argv) == 0 {
		return nil, false
	}
	switch strings.ToUpper(argv[0]) {
	case "FLUSHDB":
		return nil, true
	case "DEL":
		if len(argv) > 1 {
			return append([]string(nil), argv[1:]...), false
		}
		return nil, false
	case "SET", "INCR", "INCRBY", "EXPIRE", "PEXPIREAT",
		"LPUSH", "RPUSH", "LPOP", "RPOP", "HSET", "HDEL",
		"SADD", "SREM", "ZADD", "ZREM":
		if len(argv) > 1 {
			return []string{argv[1]}, false
		}
	}
	return nil, false
}

func (c *clientSession) String() string { return fmt.Sprintf("client-%d", c.id) }
