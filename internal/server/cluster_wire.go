package server

import (
	"context"
	"errors"
	"io"
	"net"

	"github.com/1998x-stack/mini-redis/internal/cluster"
	"github.com/1998x-stack/mini-redis/internal/resp"
)

// ServeCluster serves a cluster-aware RESP2 endpoint on the supplied listener.
// It is intentionally separate from the historical standalone listener so the
// v0.10 routing layer stays explicit and version-local. Each TCP connection owns
// one cluster.Session, which makes ASKING a true one-request connection grant.
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
	session := &cluster.Session{}
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
			_ = writer.WriteValue(resp.Error("ERR " + err.Error()))
			return
		}
		if err := writer.WriteValue(s.ClusterExecute(topology, localID, session, argv)); err != nil {
			return
		}
	}
}
