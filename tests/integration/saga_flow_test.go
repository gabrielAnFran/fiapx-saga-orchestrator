//go:build integration

// Package integration is a placeholder for a future testcontainers-go
// suite exercising SagaRepository/OutboxRepository/ProcessedEventRepository
// against a real Postgres, and messaging.Conn against a real RabbitMQ —
// mirroring pos-saga-orchestrator/tests/integration/saga_flow_test.go.
// Deferred as a stretch goal for this hackathon build; internal/domain/saga
// and internal/application/usecases already have full unit coverage
// against in-memory fakes (see state_machine_test.go, handle_event_test.go).
package integration

import "testing"

func TestPlaceholder_IntegrationSuiteNotYetImplemented(t *testing.T) {
	t.Skip("integration suite not yet implemented; see package doc comment")
}
