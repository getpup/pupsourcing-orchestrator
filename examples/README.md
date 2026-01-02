# Examples

This directory contains example applications demonstrating how to use pupsourcing-orchestrator.

## Basic Example

The [basic](./basic/) example demonstrates the fundamental usage of the orchestrator:
- Creating scoped projections that filter by aggregate type and bounded context
- Configuring the orchestrator with the Recreate strategy
- Running projections with graceful shutdown handling

To run the example:

```bash
cd examples/basic
go run main.go
```

Note: This example uses mock projections for demonstration. In a real application, you would:
1. Implement actual projection logic that writes to databases or other systems
2. Configure connection to your event store
3. Set up proper error handling and monitoring
