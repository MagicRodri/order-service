package eventing

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Message is one decoded Kafka record.
type Message struct {
	Topic   string
	Key     string
	Headers map[string]string
	// Value is nil for tombstones (deletes, and outbox cleanup records).
	Value map[string]any
}

// EventType returns the outbox event type. The Debezium outbox router copies it
// into a header, so a consumer can dispatch without inspecting the payload.
func (m Message) EventType() string {
	if t := m.Headers["eventType"]; t != "" {
		return t
	}
	return String(m.Value, "event_type")
}

type Handler func(context.Context, Message) error

type Consumer struct {
	client  *kgo.Client
	decoder *Decoder
	handler Handler
	log     *slog.Logger

	maxAttempts int
	retryDelay  time.Duration
}

// Subscription describes what a consumer reads: either an explicit topic list
// or a single regular expression.
type Subscription interface {
	IsPattern() bool
	Values() []string
	String() string
}

func NewConsumer(brokers []string, group string, sub Subscription, decoder *Decoder, handler Handler, log *slog.Logger) (*Consumer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(sub.Values()...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// Offsets are committed only after a batch has been handled, which
		// gives at-least-once delivery. Handlers deduplicate on event_id.
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	}
	if sub.IsPattern() {
		// In regex mode the client re-resolves the pattern on every metadata
		// refresh, so a topic created later — a new captured table, a new
		// outbox channel — is picked up without a restart.
		opts = append(opts, kgo.ConsumeRegex())
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &Consumer{
		client:      client,
		decoder:     decoder,
		handler:     handler,
		log:         log.With("group", group, "subscription", sub.String()),
		maxAttempts: 5,
		retryDelay:  time.Second,
	}, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	defer c.client.Close()

	for {
		fetches := c.client.PollRecords(ctx, 200)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			if !errors.Is(err, context.Canceled) {
				c.log.Error("fetch failed", "topic", topic, "partition", partition, "error", err)
			}
		})

		var handled []*kgo.Record
		fetches.EachRecord(func(r *kgo.Record) {
			if ctx.Err() != nil {
				return
			}
			if err := c.process(ctx, r); err != nil {
				// The record is left uncommitted only until the next successful
				// batch; a permanently failing record would otherwise stall the
				// partition forever, so it is skipped and reported loudly.
				c.log.Error("dropping record after repeated failures",
					"topic", r.Topic, "partition", r.Partition, "offset", r.Offset, "error", err)
			}
			handled = append(handled, r)
		})
		c.client.AllowRebalance()

		if len(handled) == 0 {
			continue
		}
		if err := c.client.CommitRecords(ctx, handled...); err != nil && ctx.Err() == nil {
			c.log.Error("commit failed", "error", err)
		}
	}
}

func (c *Consumer) process(ctx context.Context, r *kgo.Record) error {
	msg := Message{Topic: r.Topic, Key: string(r.Key), Headers: map[string]string{}}
	for _, h := range r.Headers {
		msg.Headers[h.Key] = string(h.Value)
	}

	if len(r.Value) > 0 {
		decoded, err := c.decoder.Decode(ctx, r.Value)
		if err != nil {
			return err
		}
		msg.Value = decoded
	}

	var lastErr error
	for attempt := 1; attempt <= c.maxAttempts; attempt++ {
		lastErr = c.handler(ctx, msg)
		if lastErr == nil {
			return nil
		}
		if ctx.Err() != nil {
			return lastErr
		}
		c.log.Warn("handler failed, retrying",
			"topic", r.Topic, "offset", r.Offset, "attempt", attempt, "error", lastErr)
		select {
		case <-ctx.Done():
			return lastErr
		case <-time.After(c.retryDelay * time.Duration(attempt)):
		}
	}
	return lastErr
}
