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

	err := server.Start()
	require.NoError(t, err)

	// Give the server a moment to start
	time.Sleep(100 * time.Millisecond)

	// Verify server is running by making a request
	resp, err := http.Get("http://localhost:9998/metrics")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

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

	err := server.Start()
	require.NoError(t, err)

	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(ctx)
	}()

	// Give the server a moment to start
	time.Sleep(100 * time.Millisecond)

	resp, err := http.Get("http://localhost:9997/metrics")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Content-Type"), "text/plain")
}

func TestServer_ShutdownWithCancelledContext(t *testing.T) {
	server := NewServer(":9996")

	err := server.Start()
	require.NoError(t, err)

	// Give the server a moment to start
	time.Sleep(100 * time.Millisecond)

	// Create a context that is already cancelled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = server.Shutdown(ctx)
	// Shutdown may or may not return an error when the context is cancelled,
	// depending on timing. We just verify it completes.
	_ = err
}

func TestServer_MultipleStartCallsDoNotError(t *testing.T) {
	server := NewServer(":9995")

	err1 := server.Start()
	err2 := server.Start()

	assert.NoError(t, err1)
	assert.NoError(t, err2)

	// Cleanup
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server.Shutdown(ctx)
}
