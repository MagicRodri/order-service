package config

import (
	"fmt"
	"os"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr          string
	DatabaseURL       string
	KafkaBrokers      []string
	SchemaRegistryURL string
	ConsumerGroup     string

	// BusinessTopic carries the customer domain's outbox events, produced by the
	// Debezium outbox event router. This is the only cross-service contract.
	BusinessTopic string
	// TechnicalTopic carries raw CDC rows for this service's own tables. It is
	// consumed for audit only; business decisions must never depend on it.
	TechnicalTopic string

	ShutdownTimeout time.Duration
}

func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:          env("HTTP_ADDR", ":8080"),
		DatabaseURL:       env("DATABASE_URL", ""),
		KafkaBrokers:      strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ","),
		SchemaRegistryURL: env("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
		ConsumerGroup:     env("CONSUMER_GROUP", "order-service"),
		BusinessTopic:     env("BUSINESS_TOPIC", "business.customer.events"),
		TechnicalTopic:    env("TECHNICAL_TOPIC", "tech.order.public.orders"),
		ShutdownTimeout:   15 * time.Second,
	}
	if cfg.DatabaseURL == "" {
		return cfg, fmt.Errorf("DATABASE_URL is required")
	}
	return cfg, nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
