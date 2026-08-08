package app

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/MagicRodri/order-service/internal/domain"
	"github.com/MagicRodri/order-service/internal/store"
	"github.com/google/uuid"
)

type App struct {
	store *store.Store
	log   *slog.Logger
}

func New(s *store.Store, log *slog.Logger) *App {
	return &App{store: s, log: log}
}

func (a *App) GetOrder(ctx context.Context, id string) (domain.Order, error) {
	return a.store.GetOrder(ctx, id)
}

func (a *App) ListOrders(ctx context.Context, customerID string, limit int) ([]domain.Order, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	return a.store.ListOrders(ctx, customerID, limit)
}

func (a *App) GetCustomerView(ctx context.Context, customerID string) (domain.CustomerView, error) {
	return a.store.GetCustomerView(ctx, customerID)
}

// CreateOrder authorises and prices the order against the locally replicated
// customer view, then writes the order row and its OrderCreated outbox row in
// one transaction.
//
// The authorisation check reads local state, never the customer service. That
// is the point of the pattern: an order can still be placed while the customer
// service is down, at the cost of acting on a view that may be seconds stale.
func (a *App) CreateOrder(ctx context.Context, customerID string, items []domain.Item, currency string) (domain.Order, error) {
	customerID = strings.TrimSpace(customerID)
	if _, err := uuid.Parse(customerID); err != nil {
		return domain.Order{}, fmt.Errorf("%w: customer_id must be a UUID", domain.ErrInvalidInput)
	}
	if len(items) == 0 {
		return domain.Order{}, fmt.Errorf("%w: at least one item is required", domain.ErrInvalidInput)
	}
	for i, item := range items {
		if strings.TrimSpace(item.SKU) == "" {
			return domain.Order{}, fmt.Errorf("%w: items[%d].sku is required", domain.ErrInvalidInput, i)
		}
		if item.Quantity <= 0 {
			return domain.Order{}, fmt.Errorf("%w: items[%d].quantity must be positive", domain.ErrInvalidInput, i)
		}
		if item.UnitPriceCents < 0 {
			return domain.Order{}, fmt.Errorf("%w: items[%d].unit_price_cents must not be negative", domain.ErrInvalidInput, i)
		}
	}
	if currency == "" {
		currency = "EUR"
	}

	var created domain.Order

	err := a.store.InTx(ctx, func(ctx context.Context, tx *store.Tx) error {
		customer, err := tx.GetCustomerViewForUpdate(ctx, customerID)
		if err != nil {
			return err
		}
		if customer.Blocked() {
			return domain.ErrCustomerBlocked
		}

		subtotal, discount, total := domain.Price(items, customer.DiscountBps)
		now := time.Now().UTC()
		order := domain.Order{
			ID:            uuid.NewString(),
			CustomerID:    customerID,
			Status:        domain.StatusConfirmed,
			Items:         items,
			SubtotalCents: subtotal,
			DiscountCents: discount,
			TotalCents:    total,
			Currency:      currency,
			CustomerTier:  customer.Tier,
			CreatedAt:     now,
			UpdatedAt:     now,
		}

		if err := tx.InsertOrder(ctx, order); err != nil {
			return err
		}
		created = order

		return tx.AppendOutbox(ctx, store.OutboxRecord{
			AggregateType: aggregateTypeOrder,
			AggregateID:   order.ID,
			EventType:     "OrderCreated",
			Payload:       newOrderCreated(order),
		})
	})
	if err != nil {
		return domain.Order{}, err
	}
	return created, nil
}

func (a *App) CancelOrder(ctx context.Context, id, reason string) (domain.Order, error) {
	var cancelled domain.Order

	err := a.store.InTx(ctx, func(ctx context.Context, tx *store.Tx) error {
		order, err := tx.GetOrderForUpdate(ctx, id)
		if err != nil {
			return err
		}
		if order.Status == domain.StatusCancelled {
			return domain.ErrAlreadyCancelled
		}

		now := time.Now().UTC()
		if err := tx.UpdateOrderStatus(ctx, order.ID, domain.StatusCancelled, now); err != nil {
			return err
		}
		order.Status = domain.StatusCancelled
		order.UpdatedAt = now
		cancelled = order

		return tx.AppendOutbox(ctx, store.OutboxRecord{
			AggregateType: aggregateTypeOrder,
			AggregateID:   order.ID,
			EventType:     "OrderCancelled",
			Payload: orderCancelled{
				EventID:    uuid.NewString(),
				EventType:  "OrderCancelled",
				OccurredAt: nowRFC3339(),
				OrderID:    order.ID,
				CustomerID: order.CustomerID,
				TotalCents: order.TotalCents,
				Reason:     reason,
			},
		})
	})
	if err != nil {
		return domain.Order{}, err
	}
	return cancelled, nil
}
