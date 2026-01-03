package metrics

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewServer_CreatesServerWithAddress(t *testing.T) {
	server := NewServer(":9999")

	assert.NotNil(t, server)
	assert.NotNil(t, server.server)
	assert.Equal(t, ":9999", server.server.Addr)
}

func TestServer_StartAndShutdown(t *testing.T) {
	server := NewServer(":9998")

	server.Start()

	// Give the server a moment to start
	time.Sleep(100 * time.Millisecond)

	// Check for startup errors
	assert.NoError(t, server.Err())

	// Verify server is running by making a request
	resp, err := http.Get("http://localhost:9998/metrics")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	_ = resp.Body.Close()

	// Shutdown the server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = server.Shutdown(ctx)
	assert.NoError(t, err)

	// Verify server is no longer accepting connections
	time.Sleep(100 * time.Millisecond)
	_, err = http.Get("http://localhost:9998/metrics")
	assert.Error(t, err)
}

func TestServer_MetricsEndpointReturnsPrometheusFormat(t *testing.T) {
	server := NewServer(":9997")

	server.Start()

	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()

	// Give the server a moment to start
	time.Sleep(100 * time.Millisecond)

	resp, err := http.Get("http://localhost:9997/metrics")
	require.NoError(t, err)
	defer func() {
		_ = resp.Body.Close()
	}()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Content-Type"), "text/plain")
}

func TestServer_ShutdownWithCancelledContext(t *testing.T) {
	server := NewServer(":9996")

	server.Start()

	// Give the server a moment to start
	time.Sleep(100 * time.Millisecond)

	// Create a context that is already cancelled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := server.Shutdown(ctx)
	// Shutdown may or may not return an error when the context is cancelled,
	// depending on timing. We just verify it completes.
	_ = err
}

func TestServer_MultipleStartCallsDoNotError(t *testing.T) {
	server := NewServer(":9995")

	server.Start()
	server.Start()

	// Both calls should not panic or cause issues

	// Cleanup
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
}

func TestServer_ErrReturnsStartupErrors(t *testing.T) {
	// Start a server on a port
	server1 := NewServer(":9994")
	server1.Start()

	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server1.Shutdown(ctx)
	}()

	// Give it time to bind
	time.Sleep(100 * time.Millisecond)

	// Try to start another server on the same port
	server2 := NewServer(":9994")
	server2.Start()

	// Give it time to fail
	time.Sleep(100 * time.Millisecond)

	// Should return an error about port already in use
	err := server2.Err()
	assert.Error(t, err)
}
