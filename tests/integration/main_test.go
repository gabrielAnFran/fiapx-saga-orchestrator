//go:build integration

// Package integration is a real testcontainers-go suite exercising
// SagaRepository/OutboxRepository/ProcessedEventRepository against a real
// Postgres, and messaging.Conn against a real RabbitMQ — mirroring
// pos-saga-orchestrator/tests/integration/saga_flow_test.go and following
// the shared TestMain pattern from pos-os-service/tests/integration.
//
// Run with: go test -tags=integration ./tests/integration/...
//
// internal/domain/saga and internal/application/usecases already have full
// unit coverage against in-memory fakes (see state_machine_test.go,
// handle_event_test.go); this package covers the integration layer only.
package integration

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/gabrielAnFran/fiapx-saga-orchestrator/internal/infrastructure/db"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"gorm.io/gorm"
)

// testDSN and testAMQPURL point at the Postgres and RabbitMQ containers
// started once in TestMain and shared across every test in this package.
var (
	testDSN     string
	testAMQPURL string
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	pgContainer, dsn, err := startPostgres(ctx)
	if err != nil {
		log.Fatalf("start postgres: %v", err)
	}
	testDSN = dsn

	amqpContainer, amqpURL, err := startRabbitMQ(ctx)
	if err != nil {
		log.Fatalf("start rabbitmq: %v", err)
	}
	testAMQPURL = amqpURL

	if err := db.Migrate(testDSN, "../../migrations"); err != nil {
		log.Fatalf("run migrations: %v", err)
	}

	code := m.Run()

	_ = pgContainer.Terminate(ctx)
	_ = amqpContainer.Terminate(ctx)
	os.Exit(code)
}

func startPostgres(ctx context.Context) (testcontainers.Container, string, error) {
	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "postgres",
			"POSTGRES_PASSWORD": "postgres",
			"POSTGRES_DB":       "saga_orchestrator",
		},
		WaitingFor: wait.ForListeningPort("5432/tcp").WithStartupTimeout(60 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		return nil, "", err
	}

	host, err := c.Host(ctx)
	if err != nil {
		return nil, "", err
	}
	port, err := c.MappedPort(ctx, "5432")
	if err != nil {
		return nil, "", err
	}

	dsn := fmt.Sprintf("host=%s user=postgres password=postgres dbname=saga_orchestrator port=%s sslmode=disable", host, port.Port())
	return c, dsn, nil
}

func startRabbitMQ(ctx context.Context) (testcontainers.Container, string, error) {
	req := testcontainers.ContainerRequest{
		Image:        "rabbitmq:3-management-alpine",
		ExposedPorts: []string{"5672/tcp"},
		WaitingFor:   wait.ForListeningPort("5672/tcp").WithStartupTimeout(60 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		return nil, "", err
	}

	host, err := c.Host(ctx)
	if err != nil {
		return nil, "", err
	}
	port, err := c.MappedPort(ctx, "5672")
	if err != nil {
		return nil, "", err
	}

	return c, fmt.Sprintf("amqp://guest:guest@%s:%s/", host, port.Port()), nil
}

// newTestDB opens a fresh GORM connection to the shared test Postgres container.
func newTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	gormDB, err := db.Connect(testDSN)
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	return gormDB
}

// truncateAll clears every table so each test starts from a clean slate
// despite sharing one Postgres container across the whole package.
func truncateAll(t *testing.T, gormDB *gorm.DB) {
	t.Helper()
	for _, tbl := range []string{"processed_events", "outbox", "saga_history", "saga_instances"} {
		if err := gormDB.Exec("TRUNCATE TABLE " + tbl + " CASCADE").Error; err != nil {
			t.Fatalf("truncate %s: %v", tbl, err)
		}
	}
}
