package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/getpup/pupsourcing-orchestrator/pkg/orchestrator"
	"github.com/getpup/pupsourcing/es"
	"github.com/getpup/pupsourcing/es/adapters/postgres"
	"github.com/getpup/pupsourcing/es/projection"
)

// UserProjection is an example projection
type UserProjection struct{}

func (p *UserProjection) Name() string {
	return "user_projection"
}

func (p *UserProjection) Handle(ctx context.Context, event es.PersistedEvent) error {
	log.Printf("Processing event: %s at position %d", event.EventType, event.GlobalPosition)
	return nil
}

func main() {
	// Connect to database
	db, err := sql.Open("postgres", os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Run migrations
	if err := orchestrator.RunMigrations(db); err != nil {
		log.Fatalf("Failed to run migrations: %v", err)
	}

	// Create event store
	eventStore := postgres.NewStore(postgres.DefaultStoreConfig())

	// Create custom Prometheus registry
	registry := prometheus.NewRegistry()

	// Add standard Go metrics
	registry.MustRegister(prometheus.NewGoCollector())
	registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))

	// Add your own application metrics
	myCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "myapp_custom_metric",
		Help: "My custom metric",
	})
	registry.MustRegister(myCounter)

	// Note: The orchestrator metrics are automatically registered with the
	// default Prometheus registry. To include them in your custom registry,
	// you can use prometheus.DefaultRegisterer which shares the same metrics.
	// Alternatively, expose both registries on different endpoints.

	// Start custom HTTP server for metrics and health
	mux := http.NewServeMux()
	
	// Serve orchestrator metrics (from default registry) on /metrics
	mux.Handle("/metrics", promhttp.Handler())
	
	// Optionally, serve custom metrics on a separate endpoint
	mux.Handle("/custom-metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	
	// Add health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		// Check database connectivity
		if err := db.PingContext(r.Context()); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("Database unhealthy"))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		log.Println("Starting metrics server on :8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Metrics server error: %v", err)
		}
	}()

	// Create orchestrator
	orch, err := orchestrator.New(orchestrator.Config{
		DB:         db,
		EventStore: eventStore,
		ReplicaSet: "main-projections",
	})
	if err != nil {
		log.Fatalf("Failed to create orchestrator: %v", err)
	}

	// Handle shutdown signals
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigCh
		log.Println("Shutdown signal received")
		cancel()
		// Shutdown metrics server
		if err := server.Shutdown(context.Background()); err != nil {
			log.Printf("Error shutting down metrics server: %v", err)
		}
	}()

	// Run orchestrator
	projections := []projection.Projection{&UserProjection{}}
	log.Println("Starting orchestrator...")
	if err := orch.Run(ctx, projections); err != nil && err != context.Canceled {
		log.Fatalf("Orchestrator error: %v", err)
	}

	log.Println("Orchestrator stopped")
}
