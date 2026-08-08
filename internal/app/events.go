package app

import (
	"time"

	"github.com/MagicRodri/order-service/internal/domain"
	"github.com/google/uuid"
)

// Outbox payloads. Field names and types mirror schemas/*.avsc; the outbox
// connector expands this JSON into the Avro record published on
// business.order.events.
const aggregateTypeOrder = "order"

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
