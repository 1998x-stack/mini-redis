package server

import (
	"strings"
	"time"

	"github.com/1998x-stack/mini-redis/internal/resp"
)

// executeObservedClientCommand wraps the established session command path. The
// observer records after-the-fact outcomes only; it never participates in
// validation, routing, persistence or mutation decisions.
func (s *Server) executeObservedClientCommand(session *clientSession, argv []string) (replies []resp.Value) {
	start := time.Now()
	name := "(empty)"
	protocol := resp.Protocol2
	if session != nil {
		protocol = session.protocolMode()
	}
	if len(argv) > 0 {
		name = strings.ToUpper(argv[0])
	}
	s.metrics.Commands.Inc(name)
	s.metrics.ProtocolRequests.Inc(protocolMetricKey(protocol))
	defer func() {
		s.metrics.CommandLatency.ObserveDuration(time.Since(start))
		if repliesContainError(replies) {
			s.metrics.CommandErrors.Inc(name)
		}
	}()

	if name == "INFO" {
		if session == nil {
			return []resp.Value{resp.Error("ERR INFO requires a client session")}
		}
		if session.subscribed() && session.protocolMode() == resp.Protocol2 {
			return []resp.Value{resp.Error("ERR only SUBSCRIBE, UNSUBSCRIBE and PING are allowed in subscribed RESP2 mode")}
		}
		session.mu.Lock()
		inMulti := session.inMulti
		session.mu.Unlock()
		if inMulti {
			return []resp.Value{resp.Error("ERR INFO must be called outside MULTI")}
		}
		return []resp.Value{s.infoCommand(argv)}
	}
	return s.executeClientCommand(session, argv)
}

func (s *Server) recordClusterCommand(protocol resp.Protocol, argv []string, reply resp.Value, started time.Time) {
	name := "(empty)"
	if len(argv) > 0 {
		name = strings.ToUpper(argv[0])
	}
	s.metrics.Commands.Inc(name)
	s.metrics.ProtocolRequests.Inc(protocolMetricKey(protocol))
	s.metrics.CommandLatency.ObserveDuration(time.Since(started))
	if reply.IsError() {
		s.metrics.CommandErrors.Inc(name)
	}
}
