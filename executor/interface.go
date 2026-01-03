package executor

import (
	"context"

	"github.com/getpup/pupsourcing-orchestrator"
	"github.com/getpup/pupsourcing/es/projection"
)

// Runner executes projections with a partition assignment.
// This interface allows for mock implementations in tests.
type Runner interface {
	Run(ctx context.Context, projections []projection.Projection, assignment orchestrator.PartitionAssignment) error
}
