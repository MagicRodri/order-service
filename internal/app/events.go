package app

import (
	"time"

	"github.com/MagicRodri/order-service/internal/domain"
	"github.com/google/uuid"
)

// Outbox payloads. Field names and types mirror schemas/*.avsc; the outbox
// connector expands this JSON into the Avro record published on the channel's
// topic.
const aggregateTypeOrder = "order"

// Channels split this domain's events across topics. The connector routes
// purely on the channel column and knows nothing about event types, so carving
// out a new topic is a change to this table alone — no connector edit, and no
// interruption for consumers that subscribe by pattern.
//
// Lifecycle and settlement are separated because they have different
// audiences: the customer service follows lifecycle to track spend, while a
// billing or accounting consumer would follow settlement alone.
const (
	channelOrderLifecycle  = "order.lifecycle"  // → business.order.lifecycle.events
	channelOrderSettlement = "order.settlement" // → business.order.settlement.events
)

// channelFor maps an event type to its topic. An unrecognised type falls back
// to the domain-wide channel rather than landing somewhere unroutable.
func channelFor(eventType string) string {
	switch eventType {
	case "OrderCreated":
		return channelOrderLifecycle
	case "OrderCancelled":
		return channelOrderSettlement
	default:
		return aggregateTypeOrder
	}
}

type orderCreated struct {
	EventID       string `json:"event_id"`
	EventType     string `json:"event_type"`
	OccurredAt    string `json:"occurred_at"`
	OrderID       string `json:"order_id"`
	CustomerID    string `json:"customer_id"`
	SubtotalCents int64  `json:"subtotal_cents"`
	DiscountCents int64  `json:"discount_cents"`
	TotalCents    int64  `json:"total_cents"`
	Currency      string `json:"currency"`
	CustomerTier  string `json:"customer_tier"`
	ItemCount     int32  `json:"item_count"`
}

type orderCancelled struct {
	EventID    string `json:"event_id"`
	EventType  string `json:"event_type"`
	OccurredAt string `json:"occurred_at"`
	OrderID    string `json:"order_id"`
	CustomerID string `json:"customer_id"`
	TotalCents int64  `json:"total_cents"`
	Reason     string `json:"reason"`
}

func newOrderCreated(o domain.Order) orderCreated {
	return orderCreated{
		EventID:       uuid.NewString(),
		EventType:     "OrderCreated",
		OccurredAt:    nowRFC3339(),
		OrderID:       o.ID,
		CustomerID:    o.CustomerID,
		SubtotalCents: o.SubtotalCents,
		DiscountCents: o.DiscountCents,
		TotalCents:    o.TotalCents,
		Currency:      o.Currency,
		CustomerTier:  o.CustomerTier,
		ItemCount:     int32(len(o.Items)),
	}
}

func nowRFC3339() string { return time.Now().UTC().Format(time.RFC3339Nano) }
