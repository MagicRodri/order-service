CREATE TABLE IF NOT EXISTS orders (
    id             UUID PRIMARY KEY,
    customer_id    UUID        NOT NULL,
    status         TEXT        NOT NULL DEFAULT 'CONFIRMED',
    items          JSONB       NOT NULL,
    subtotal_cents BIGINT      NOT NULL,
    discount_cents BIGINT      NOT NULL DEFAULT 0,
    total_cents    BIGINT      NOT NULL,
    currency       TEXT        NOT NULL DEFAULT 'EUR',
    customer_tier  TEXT        NOT NULL DEFAULT 'STANDARD',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- REPLICA IDENTITY FULL makes Postgres log the pre-image of every row, so the
-- technical CDC stream carries a populated `before` block on UPDATE/DELETE.
ALTER TABLE orders REPLICA IDENTITY FULL;

-- Transactional outbox. Rows are written in the same transaction as the state
-- change they describe, then picked up by the Debezium outbox connector and
-- routed onto business.order.events.
CREATE TABLE IF NOT EXISTS outbox (
    id             UUID PRIMARY KEY,
    aggregate_type TEXT        NOT NULL,
    aggregate_id   TEXT        NOT NULL,
    event_type     TEXT        NOT NULL,
    payload        JSONB       NOT NULL,
    trace_id       TEXT        NOT NULL DEFAULT '',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Local replica of the customer facts needed to authorise and price an order,
-- fed exclusively by business.customer.events.
CREATE TABLE IF NOT EXISTS customer_view (
    customer_id  UUID PRIMARY KEY,
    status       TEXT        NOT NULL DEFAULT 'ACTIVE',
    tier         TEXT        NOT NULL DEFAULT 'STANDARD',
    discount_bps INTEGER     NOT NULL DEFAULT 0,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Consumed event IDs, recorded inside the handler's transaction so that a
-- redelivery after a crash cannot apply the same effect twice.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     UUID PRIMARY KEY,
    source_topic TEXT        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Landing table for the technical (raw CDC) stream. Audit only.
CREATE TABLE IF NOT EXISTS technical_audit_log (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source      TEXT        NOT NULL,
    operation   TEXT        NOT NULL,
    row_key     TEXT        NOT NULL,
    before_row  JSONB,
    after_row   JSONB,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_outbox_created_at ON outbox (created_at);
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders (customer_id);
