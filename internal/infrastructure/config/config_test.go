package config

import "testing"

func TestLoad_Defaults(t *testing.T) {
	t.Setenv("SAGA_PORT", "")
	t.Setenv("SAGA_DB_DSN", "")
	t.Setenv("SAGA_AMQP_URL", "")
	t.Setenv("SAGA_DISPATCH_INTERVAL_MS", "")

	cfg := Load()

	if cfg.Port != "8084" {
		t.Errorf("Port = %q, want %q", cfg.Port, "8084")
	}
	if cfg.DBDSN != "host=localhost user=postgres password=postgres dbname=saga_orchestrator port=5432 sslmode=disable" {
		t.Errorf("unexpected default DBDSN: %q", cfg.DBDSN)
	}
	if cfg.AMQPURL != "amqp://guest:guest@localhost:5672/" { //nolint:gosec // default RabbitMQ dev credentials, not a real secret
		t.Errorf("unexpected default AMQPURL: %q", cfg.AMQPURL)
	}
	if cfg.DispatchIntervalMS != 500 {
		t.Errorf("DispatchIntervalMS = %d, want 500", cfg.DispatchIntervalMS)
	}
}

func TestLoad_EnvOverrides(t *testing.T) {
	t.Setenv("SAGA_PORT", "9999")
	t.Setenv("SAGA_DB_DSN", "custom-dsn")
	t.Setenv("SAGA_AMQP_URL", "amqp://custom/")
	t.Setenv("SAGA_DISPATCH_INTERVAL_MS", "1500")

	cfg := Load()

	if cfg.Port != "9999" {
		t.Errorf("Port = %q, want %q", cfg.Port, "9999")
	}
	if cfg.DBDSN != "custom-dsn" {
		t.Errorf("DBDSN = %q, want %q", cfg.DBDSN, "custom-dsn")
	}
	if cfg.AMQPURL != "amqp://custom/" {
		t.Errorf("AMQPURL = %q, want %q", cfg.AMQPURL, "amqp://custom/")
	}
	if cfg.DispatchIntervalMS != 1500 {
		t.Errorf("DispatchIntervalMS = %d, want 1500", cfg.DispatchIntervalMS)
	}
}

func TestLoad_InvalidIntFallsBackToDefault(t *testing.T) {
	t.Setenv("SAGA_DISPATCH_INTERVAL_MS", "not-a-number")

	cfg := Load()

	if cfg.DispatchIntervalMS != 500 {
		t.Errorf("DispatchIntervalMS = %d, want default 500 on parse error", cfg.DispatchIntervalMS)
	}
}
