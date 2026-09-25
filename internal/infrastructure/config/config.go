package config

import (
	"os"
	"strconv"
)

type Config struct {
	Port               string
	DBDSN              string
	AMQPURL            string
	DispatchIntervalMS int
}

func Load() Config {
	return Config{
		Port:               getEnv("SAGA_PORT", "8084"),
		DBDSN:              getEnv("SAGA_DB_DSN", "host=localhost user=postgres password=postgres dbname=saga_orchestrator port=5432 sslmode=disable"),
		AMQPURL:            getEnv("SAGA_AMQP_URL", "amqp://guest:guest@localhost:5672/"),
		DispatchIntervalMS: getEnvInt("SAGA_DISPATCH_INTERVAL_MS", 500),
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
