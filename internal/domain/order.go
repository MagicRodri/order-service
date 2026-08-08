package domain

import (
	"errors"
	"time"
)

type Status string

const (
	StatusConfirmed Status = "CONFIRMED"
	StatusCancelled Status = "CANCELLED"
)

var (
	ErrNotFound         = errors.New("order not found")
	ErrInvalidInput     = errors.New("invalid input")
	ErrUnknownCustomer  = errors.New("customer is unknown to this service")
	ErrCustomerBlocked  = errors.New("customer is blocked")
	ErrAlreadyCancelled = errors.New("order is already cancelled")
)

type Item struct {
	SKU            string `json:"sku"`
	Quantity       int32  `json:"quantity"`
	UnitPriceCents int64  `json:"unit_price_cents"`
}

type Order struct {
	ID             string    `json:"id"`
	CustomerID     string    `json:"customer_id"`
	Status         Status    `json:"status"`
	Items          []Item    `json:"items"`
	SubtotalCents  int64     `json:"subtotal_cents"`
	DiscountCents  int64     `json:"discount_cents"`
	TotalCents     int64     `json:"total_cents"`
	Currency       string    `json:"currency"`
	CustomerTier   string    `json:"customer_tier"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// CustomerView is this service's local replica of the customer facts it needs
// to price and authorise an order. It is built only from business events; the
// customer service's tables are never read directly.
type CustomerView struct {
	CustomerID  string    `json:"customer_id"`
	Status      string    `json:"status"`
	Tier        string    `json:"tier"`
	DiscountBps int32     `json:"discount_bps"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func (v CustomerView) Blocked() bool { return v.Status == "BLOCKED" }

// Price computes the order total from its items and the discount the customer's
// tier earns. Discounts are basis points, so the division is by 10 000.
func Price(items []Item, discountBps int32) (subtotal, discount, total int64) {
	for _, item := range items {
		subtotal += item.UnitPriceCents * int64(item.Quantity)
	}
	discount = subtotal * int64(discountBps) / 10_000
	return subtotal, discount, subtotal - discount
}
