package app

import (
	"context"
	"fmt"
	"time"

	"github.com/MagicRodri/order-service/internal/domain"
	"github.com/MagicRodri/order-service/internal/eventing"
	"github.com/MagicRodri/order-service/internal/store"
)

// HandleBusinessEvent projects the customer domain's outbox events into the
// local customer view. These are versioned contracts owned by the customer
// service; nothing here depends on that service's table layout.
func (a *App) HandleBusinessEvent(ctx context.Context, msg eventing.Message) error {
	if msg.Value == nil {
		return nil // tombstone from outbox retention cleanup
	}

	eventType := msg.EventType()
	eventID := eventing.String(msg.Value, "event_id")
	customerID := eventing.String(msg.Value, "customer_id")

	switch eventType {
	case "CustomerCreated", "CustomerBlocked", "CustomerUnblocked", "CustomerTierChanged":
	default:
		a.log.Debug("ignoring business event", "type", eventType, "topic", msg.Topic)
		return nil
	}
	if eventID == "" || customerID == "" {
		return fmt.Errorf("malformed %s: event_id=%q customer_id=%q; record actually carries %v",
			eventType, eventID, customerID, eventing.FieldNames(msg.Value))
	}

	return a.store.InTx(ctx, func(ctx context.Context, tx *store.Tx) error {
		fresh, err := tx.MarkProcessed(ctx, eventID, msg.Topic)
		if err != nil {
			return err
		}
		if !fresh {
			a.log.Debug("skipping already-processed event", "event_id", eventID)
			return nil
		}

		now := time.Now().UTC()
		switch eventType {
		case "CustomerCreated":
			return tx.UpsertCustomerView(ctx, domain.CustomerView{
				CustomerID:  customerID,
				Status:      eventing.String(msg.Value, "status"),
				Tier:        eventing.String(msg.Value, "tier"),
				DiscountBps: int32(eventing.Int64(msg.Value, "discount_bps")),
				UpdatedAt:   now,
			})

		case "CustomerBlocked":
			a.log.Info("customer blocked, further orders will be refused", "customer_id", customerID)
			return tx.SetCustomerStatus(ctx, customerID, "BLOCKED", now)

		case "CustomerUnblocked":
			return tx.SetCustomerStatus(ctx, customerID, "ACTIVE", now)

		case "CustomerTierChanged":
			tier := eventing.String(msg.Value, "tier")
			discount := int32(eventing.Int64(msg.Value, "discount_bps"))
			a.log.Info("customer tier changed, discount updated",
				"customer_id", customerID, "tier", tier, "discount_bps", discount)
			return tx.SetCustomerTier(ctx, customerID, tier, discount, now)
		}
		return nil
	})
}

// HandleTechnicalEvent stores raw CDC rows for this service's own tables.
//
// The technical stream mirrors the physical schema and changes whenever a
// column does, so it is deliberately confined to audit. Cross-service reactions
// belong on the business stream above.
func (a *App) HandleTechnicalEvent(ctx context.Context, msg eventing.Message) error {
	if msg.Value == nil {
		return nil // Debezium tombstone following a delete
	}

	operation := eventing.String(msg.Value, "op")
	before, _ := eventing.Record(msg.Value, "before")
	after, _ := eventing.Record(msg.Value, "after")

	beforeJSON, err := eventing.JSON(before)
	if err != nil {
		return fmt.Errorf("encode before image: %w", err)
	}
	afterJSON, err := eventing.JSON(after)
	if err != nil {
		return fmt.Errorf("encode after image: %w", err)
	}

	rowKey := eventing.String(after, "id")
	if rowKey == "" {
		rowKey = eventing.String(before, "id")
	}

	return a.store.AppendTechnicalAudit(ctx, msg.Topic, operation, rowKey, beforeJSON, afterJSON)
}
