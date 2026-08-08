package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/MagicRodri/order-service/internal/domain"
	"github.com/MagicRodri/order-service/migrations"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Tx is the unit of work handed to callers. Every method on it runs inside the
// same database transaction, which is what makes the outbox write atomic with
// the state change it describes.
type Tx struct {
	tx pgx.Tx
}

// InTx runs fn inside a transaction, committing when fn returns nil.
func (s *Store) InTx(ctx context.Context, fn func(context.Context, *Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := fn(ctx, &Tx{tx: tx}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`)
	if err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	entries, err := migrations.FS.ReadDir(".")
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	for _, name := range names {
		var exists bool
		if err := s.pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`, name,
		).Scan(&exists); err != nil {
			return fmt.Errorf("check migration %s: %w", name, err)
		}
		if exists {
			continue
		}
		body, err := migrations.FS.ReadFile(name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		if _, err := s.pool.Exec(ctx, string(body)); err != nil {
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
		if _, err := s.pool.Exec(ctx,
			`INSERT INTO schema_migrations (version) VALUES ($1)`, name); err != nil {
			return fmt.Errorf("record migration %s: %w", name, err)
		}
	}
	return nil
}

const orderColumns = `id, customer_id, status, items, subtotal_cents, discount_cents,
	total_cents, currency, customer_tier, created_at, updated_at`

func scanOrder(row pgx.Row) (domain.Order, error) {
	var o domain.Order
	var items []byte
	err := row.Scan(&o.ID, &o.CustomerID, &o.Status, &items, &o.SubtotalCents,
		&o.DiscountCents, &o.TotalCents, &o.Currency, &o.CustomerTier, &o.CreatedAt, &o.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return o, domain.ErrNotFound
	}
	if err != nil {
		return o, err
	}
	if err := json.Unmarshal(items, &o.Items); err != nil {
		return o, fmt.Errorf("decode items: %w", err)
	}
	return o, nil
}

func (s *Store) GetOrder(ctx context.Context, id string) (domain.Order, error) {
	return scanOrder(s.pool.QueryRow(ctx,
		`SELECT `+orderColumns+` FROM orders WHERE id = $1`, id))
}

func (s *Store) ListOrders(ctx context.Context, customerID string, limit int) ([]domain.Order, error) {
	query := `SELECT ` + orderColumns + ` FROM orders WHERE ($1 = '' OR customer_id::text = $1)
		ORDER BY created_at DESC LIMIT $2`
	rows, err := s.pool.Query(ctx, query, customerID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []domain.Order{}
	for rows.Next() {
		o, err := scanOrder(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

func (s *Store) GetCustomerView(ctx context.Context, customerID string) (domain.CustomerView, error) {
	return scanCustomerView(s.pool.QueryRow(ctx,
		`SELECT customer_id, status, tier, discount_bps, updated_at
		 FROM customer_view WHERE customer_id = $1`, customerID))
}

func scanCustomerView(row pgx.Row) (domain.CustomerView, error) {
	var v domain.CustomerView
	err := row.Scan(&v.CustomerID, &v.Status, &v.Tier, &v.DiscountBps, &v.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return v, domain.ErrUnknownCustomer
	}
	return v, err
}

// GetCustomerViewForUpdate locks the replica row so an inbound customer event
// and an order being priced cannot interleave.
func (t *Tx) GetCustomerViewForUpdate(ctx context.Context, customerID string) (domain.CustomerView, error) {
	return scanCustomerView(t.tx.QueryRow(ctx,
		`SELECT customer_id, status, tier, discount_bps, updated_at
		 FROM customer_view WHERE customer_id = $1 FOR UPDATE`, customerID))
}

func (t *Tx) UpsertCustomerView(ctx context.Context, v domain.CustomerView) error {
	_, err := t.tx.Exec(ctx,
		`INSERT INTO customer_view (customer_id, status, tier, discount_bps, updated_at)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (customer_id) DO UPDATE
		 SET status = EXCLUDED.status, tier = EXCLUDED.tier,
		     discount_bps = EXCLUDED.discount_bps, updated_at = EXCLUDED.updated_at`,
		v.CustomerID, v.Status, v.Tier, v.DiscountBps, v.UpdatedAt)
	return err
}

// SetCustomerStatus updates only the status, leaving tier untouched. A block
// event says nothing about the tier, so it must not overwrite it.
func (t *Tx) SetCustomerStatus(ctx context.Context, customerID, status string, at time.Time) error {
	_, err := t.tx.Exec(ctx,
		`INSERT INTO customer_view (customer_id, status, updated_at) VALUES ($1, $2, $3)
		 ON CONFLICT (customer_id) DO UPDATE
		 SET status = EXCLUDED.status, updated_at = EXCLUDED.updated_at`,
		customerID, status, at)
	return err
}

// SetCustomerTier updates only the tier and its discount, leaving status alone.
func (t *Tx) SetCustomerTier(ctx context.Context, customerID, tier string, discountBps int32, at time.Time) error {
	_, err := t.tx.Exec(ctx,
		`INSERT INTO customer_view (customer_id, tier, discount_bps, updated_at) VALUES ($1, $2, $3, $4)
		 ON CONFLICT (customer_id) DO UPDATE
		 SET tier = EXCLUDED.tier, discount_bps = EXCLUDED.discount_bps,
		     updated_at = EXCLUDED.updated_at`,
		customerID, tier, discountBps, at)
	return err
}

func (t *Tx) InsertOrder(ctx context.Context, o domain.Order) error {
	items, err := json.Marshal(o.Items)
	if err != nil {
		return fmt.Errorf("encode items: %w", err)
	}
	_, err = t.tx.Exec(ctx,
		`INSERT INTO orders (id, customer_id, status, items, subtotal_cents, discount_cents,
		                     total_cents, currency, customer_tier, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		o.ID, o.CustomerID, o.Status, items, o.SubtotalCents, o.DiscountCents,
		o.TotalCents, o.Currency, o.CustomerTier, o.CreatedAt, o.UpdatedAt)
	return err
}

func (t *Tx) GetOrderForUpdate(ctx context.Context, id string) (domain.Order, error) {
	return scanOrder(t.tx.QueryRow(ctx,
		`SELECT `+orderColumns+` FROM orders WHERE id = $1 FOR UPDATE`, id))
}

func (t *Tx) UpdateOrderStatus(ctx context.Context, id string, status domain.Status, at time.Time) error {
	_, err := t.tx.Exec(ctx,
		`UPDATE orders SET status = $2, updated_at = $3 WHERE id = $1`, id, status, at)
	return err
}

// OutboxRecord is one business event, staged for the outbox connector.
type OutboxRecord struct {
	AggregateType string
	AggregateID   string
	EventType     string
	Payload       any
	TraceID       string
}

func (t *Tx) AppendOutbox(ctx context.Context, rec OutboxRecord) error {
	payload, err := json.Marshal(rec.Payload)
	if err != nil {
		return fmt.Errorf("marshal outbox payload: %w", err)
	}
	_, err = t.tx.Exec(ctx,
		`INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, payload, trace_id, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		uuid.NewString(), rec.AggregateType, rec.AggregateID, rec.EventType,
		payload, rec.TraceID, time.Now().UTC())
	return err
}

// MarkProcessed records an inbound event ID. It reports false when the event
// was already applied, which is the consumer's deduplication check.
func (t *Tx) MarkProcessed(ctx context.Context, eventID, sourceTopic string) (bool, error) {
	tag, err := t.tx.Exec(ctx,
		`INSERT INTO processed_events (event_id, source_topic) VALUES ($1, $2)
		 ON CONFLICT (event_id) DO NOTHING`, eventID, sourceTopic)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (s *Store) AppendTechnicalAudit(ctx context.Context, source, operation, rowKey string, before, after []byte) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO technical_audit_log (source, operation, row_key, before_row, after_row)
		 VALUES ($1, $2, $3, $4, $5)`, source, operation, rowKey, before, after)
	return err
}

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }
