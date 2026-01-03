package metrics

import (
	"context"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server provides an optional HTTP server for metrics.
// Use this only if your application does not already expose metrics.
type Server struct {
	server *http.Server
}

// NewServer creates a metrics server on the specified address.
// Example address: ":9090" or "localhost:9090"
func NewServer(addr string) *Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	return &Server{
		server: &http.Server{
			Addr:    addr,
			Handler: mux,
		},
	}
}

// Start starts the metrics server in a goroutine.
// Returns immediately. Use Shutdown to stop.
func (s *Server) Start() error {
	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			// Log error - server failed to start
			// Note: In production, you should handle this error appropriately
		}
	}()
	return nil
}

// Shutdown gracefully shuts down the metrics server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}
