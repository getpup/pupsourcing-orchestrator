package metrics

import (
	"context"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server provides an optional HTTP server for metrics.
// Use this only if your application does not already expose metrics.
type Server struct {
	server  *http.Server
	errChan chan error
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
		errChan: make(chan error, 1),
	}
}

// Start starts the metrics server in a goroutine.
// Returns immediately. Check Err() to detect startup failures.
// Use Shutdown to stop the server.
func (s *Server) Start() {
	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.errChan <- err
		}
	}()
}

// Err returns any error that occurred during server startup or operation.
// This is non-blocking and returns nil if no error has occurred.
func (s *Server) Err() error {
	select {
	case err := <-s.errChan:
		return err
	default:
		return nil
	}
}

// Shutdown gracefully shuts down the metrics server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}
