package config

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"
)

// TopicSelector names the topics a consumer subscribes to.
//
// The default is a regular expression, because topic names are derived rather
// than fixed: the technical stream gets one topic per captured table, and the
// business stream gets one topic per outbox channel. A pattern picks up a new
// table or a new channel without redeploying the service.
//
// An explicit list always wins, so a service can pin exactly what it reads.
type TopicSelector struct {
	Topics  []string
	Pattern string
}

// IsPattern reports whether the selector must be consumed in regex mode.
func (s TopicSelector) IsPattern() bool { return len(s.Topics) == 0 }

// Values returns what to hand to the Kafka client: either the explicit topics
// or the single pattern.
func (s TopicSelector) Values() []string {
	if len(s.Topics) > 0 {
		return s.Topics
	}
	return []string{s.Pattern}
}

func (s TopicSelector) String() string {
	if len(s.Topics) > 0 {
		return "topics=" + strings.Join(s.Topics, ",")
	}
	return "pattern=" + s.Pattern
}

func (s TopicSelector) validate(name string) error {
	if len(s.Topics) > 0 {
		return nil
	}
	if s.Pattern == "" {
		return fmt.Errorf("%s: neither an explicit topic list nor a pattern was configured", name)
	}
	if _, err := regexp.Compile(s.Pattern); err != nil {
		return fmt.Errorf("%s: %q is not a valid regular expression: %w", name, s.Pattern, err)
	}
	return nil
}

type Config struct {
	HTTPAddr          string
	DatabaseURL       string
	KafkaBrokers      []string
	SchemaRegistryURL string
	ConsumerGroup     string

	// Business carries the customer domain's outbox events. Splitting those events
	// across several channels produces several topics, which the default
	// pattern matches as a group.
	Business TopicSelector
	// Technical carries raw CDC rows for this service's own tables, one topic
	// per table. It is consumed for audit only; business decisions must never
	// depend on it.
	Technical TopicSelector

	ShutdownTimeout time.Duration
}

func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:          env("HTTP_ADDR", ":8080"),
		DatabaseURL:       env("DATABASE_URL", ""),
		KafkaBrokers:      splitList(env("KAFKA_BROKERS", "localhost:9092")),
		SchemaRegistryURL: env("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
		ConsumerGroup:     env("CONSUMER_GROUP", "order-service"),
		Business: TopicSelector{
			Topics:  splitList(os.Getenv("BUSINESS_TOPICS")),
			Pattern: env("BUSINESS_TOPIC_PATTERN", `^business\.customer\..*`),
		},
		Technical: TopicSelector{
			Topics:  splitList(os.Getenv("TECHNICAL_TOPICS")),
			Pattern: env("TECHNICAL_TOPIC_PATTERN", `^tech\.order\..*`),
		},
		ShutdownTimeout: 15 * time.Second,
	}

	if cfg.DatabaseURL == "" {
		return cfg, fmt.Errorf("DATABASE_URL is required")
	}
	if err := cfg.Business.validate("BUSINESS_TOPIC_PATTERN"); err != nil {
		return cfg, err
	}
	if err := cfg.Technical.validate("TECHNICAL_TOPIC_PATTERN"); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func splitList(raw string) []string {
	var out []string
	for _, part := range strings.Split(raw, ",") {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
