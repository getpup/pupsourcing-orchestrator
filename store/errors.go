package store

import "errors"

var (
	// ErrWorkerNotFound indicates the worker does not exist.
	ErrWorkerNotFound = errors.New("worker not found")

	// ErrGenerationNotFound indicates the generation does not exist.
	ErrGenerationNotFound = errors.New("generation not found")
)
