package main

import (
	"fmt"
	"log"

	"github.com/getpup/pupsourcing-orchestrator/internal/orchestrator"
	"github.com/getpup/pupsourcing-orchestrator/pkg/version"
)

func main() {
	log.Printf("Starting pupsourcing-orchestrator v%s", version.Version)

	orch := orchestrator.New()
	if err := orch.Run(); err != nil {
		log.Fatalf("Failed to run orchestrator: %v", err)
	}

	fmt.Println("Orchestrator completed successfully")
}
