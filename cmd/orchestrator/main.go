package main

import (
	"fmt"
	"log"

	"github.com/getpup/pupsourcing-orchestrator/pkg/version"
)

func main() {
	log.Printf("pupsourcing-orchestrator v%s", version.Version)
	fmt.Println("This is a library package. See examples/ directory for usage examples.")
	fmt.Println("Visit: https://github.com/getpup/pupsourcing-orchestrator/tree/main/examples")
}
