//go:build !linux

package server

import (
	"context"
	"fmt"
)

func (s *Server) serveReactor(context.Context) error {
	return fmt.Errorf("reactor network engine requires Linux epoll; use -network-engine=legacy or run Linux/Docker")
}
