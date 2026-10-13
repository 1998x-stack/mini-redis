package server

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"

	"github.com/1998x-stack/mini-redis/internal/cluster"
	"github.com/1998x-stack/mini-redis/internal/resp"
)

// ServeCluster serves the explicit cluster-aware endpoint introduced in v0.10.
// v0.11 adds HELLO 2/3 negotiation here without changing the routing boundary:
// every ordinary key command still passes through ClusterExecute before Store.
func (s *Server) ServeCluster(ctx context.Context, listener net.Listener, topology *cluster.Topology, localID string) error {
	if listener == nil || topology == nil || localID == "" {
		return errors.New("cluster listener, topology and local node id are required")
	}
	closed := make(chan struct{})
	defer close(closed)
	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
		case <-closed:
		}
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go s.serveClusterConn(ctx, conn, topology, localID)
	}
}

func (s *Server) serveClusterConn(ctx context.Context, conn net.Conn, topology *cluster.Topology, localID string) {
	defer conn.Close()
	clusterSession := &cluster.Session{}
	protocolSession := s.newClientSession(nil)
	defer s.closeClientSession(protocolSession)
	reader := resp.NewReader(conn)
	writer := resp.NewWriter(conn)
	for {
		if ctx.Err() != nil {
			return
		}
		argv, err := reader.ReadCommand()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			_ = writer.WriteValueProtocol(resp.Error("ERR "+err.Error()), protocolSession.protocolMode())
			return
		}

		if len(argv) > 0 && strings.EqualFold(argv[0], "HELLO") {
			reply := setHelloMode(s.helloCommand(protocolSession, argv), "cluster")
			if err := writer.WriteValueProtocol(reply, protocolSession.protocolMode()); err != nil {
				return
			}
			continue
		}

		reply := s.ClusterExecute(topology, localID, clusterSession, argv)
		reply = s.adaptReplyForProtocol(protocolSession, argv, reply)
		if err := writer.WriteValueProtocol(reply, protocolSession.protocolMode()); err != nil {
			return
		}
	}
}

func setHelloMode(reply resp.Value, mode string) resp.Value {
	if reply.Type != resp.MapType {
		return reply
	}
	for i := range reply.Map {
		if reply.Map[i].Key.Type == resp.BulkStringType && reply.Map[i].Key.Str == "mode" {
			reply.Map[i].Value = resp.BulkString(mode)
			break
		}
	}
	return reply
}
