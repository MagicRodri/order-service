# order-service

Order domain microservice, written in Go. It owns orders, publishes its state
changes as Avro events, and keeps a local replica of the customer facts it needs
to authorise and price an order.

It is designed to run inside the
[`eda_microservices`](https://github.com/MagicRodri/eda_microservices) monorepo,
which supplies Kafka, Schema Registry, Kafka Connect and the connector
configuration. Nothing here talks to Kafka to *produce*: publication is the
outbox connector's job.

## The two event streams

This service is on both sides of both streams, and they are not
interchangeable.

**Technical events** are raw change-data-capture rows for the `orders` table,
published by a Debezium Postgres connector to `tech.order.public.orders`. Their
shape is the physical table: a column rename changes the event. They are
consumed here only to fill `technical_audit_log`. No business decision may
depend on them.

**Business events** are explicit, versioned facts — `OrderCreated`,
`OrderCancelled` — whose contracts live in [`schemas/`](schemas). They are
written to the `outbox` table and routed by the Debezium outbox event router to
`business.order.events`. This is the only surface other services are allowed to
couple to.

## Why the outbox

Writing to Postgres and publishing to Kafka are two systems, so doing both
directly means one can succeed while the other fails, and the event stream
starts lying about the database. The outbox removes the second system from the
write path: the state change and the event describing it are inserted in the
same transaction, so they commit or roll back together.

```go
err := a.store.InTx(ctx, func(ctx context.Context, tx *store.Tx) error {
    if err := tx.InsertOrder(ctx, order); err != nil {
        return err
    }
    return tx.AppendOutbox(ctx, store.OutboxRecord{ /* OrderCreated */ })
})
```

Publication then happens out-of-band: the connector tails the WAL, so an event
that committed will eventually reach Kafka even if this process dies the instant
after `COMMIT`.

That gives at-least-once delivery, not exactly-once. Consumers deduplicate: each
payload carries an `event_id`, and handlers insert it into `processed_events`
inside the very transaction that applies the effect, so a redelivery is a no-op.

## No synchronous call to customer-service

`POST /orders` never calls the customer service. It reads `customer_view`, a
local table built exclusively from `business.customer.events`:

| Event                 | Effect on `customer_view`                 |
| --------------------- | ----------------------------------------- |
| `CustomerCreated`     | Insert the row (status, tier, discount)   |
| `CustomerBlocked`     | `status = BLOCKED` — later orders refused  |
| `CustomerUnblocked`   | `status = ACTIVE`                          |
| `CustomerTierChanged` | New `tier` and `discount_bps` for pricing  |

Block and tier events update disjoint columns, so an out-of-order block never
clobbers a tier change and vice versa.

The trade-off is explicit: orders can still be placed while the customer service
is down, at the cost of acting on a view that may be seconds stale. A customer
blocked a moment ago may get one more order through — which is why
`GET /customer-view/{id}` exists, to make the replica's lag observable.

An order for a customer the view has never seen returns **409**, not 404: the
customer may well exist, its `CustomerCreated` event simply has not arrived yet,
so the caller should retry.

## Pricing

`domain.Price` sums the items and applies the tier discount in basis points:

```
discount = subtotal * discount_bps / 10000
total    = subtotal - discount
```

Integer division rounds the discount down, so the total can never exceed the
subtotal. GOLD is 500 bps (5%), PLATINUM 1000 bps (10%).

## API

| Method | Path                     | Effect                                        |
| ------ | ------------------------ | --------------------------------------------- |
| POST   | `/orders`                | Authorise, price, persist; emits `OrderCreated`|
| GET    | `/orders`                | List (`?customer_id=`, `?limit=`)              |
| GET    | `/orders/{id}`           | Fetch one                                      |
| POST   | `/orders/{id}/cancel`    | Emits `OrderCancelled` (reverses spend)        |
| GET    | `/customer-view/{id}`    | Inspect the replicated customer facts          |
| GET    | `/healthz`               | Liveness                                       |

```bash
curl -X POST localhost:8092/orders \
  -H 'content-type: application/json' \
  -d '{"customer_id":"<uuid>","items":[{"sku":"book","quantity":2,"unit_price_cents":1500}]}'
```

## Configuration

| Variable              | Default                      |
| --------------------- | ---------------------------- |
| `HTTP_ADDR`           | `:8080`                      |
| `DATABASE_URL`        | *(required)*                 |
| `KAFKA_BROKERS`       | `localhost:9092`             |
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081`      |
| `CONSUMER_GROUP`      | `order-service`              |
| `BUSINESS_TOPIC`      | `business.customer.events`   |
| `TECHNICAL_TOPIC`     | `tech.order.public.orders`   |
| `LOG_LEVEL`           | `info` (`debug` for verbose) |

Migrations in [`migrations/`](migrations) are embedded in the binary and applied
at startup.

## Consuming Avro

Messages are decoded with the **writer's** schema, fetched from the registry by
the ID in the Confluent wire header (`internal/eventing/decoder.go`). Decoding
with the writer's schema rather than a compiled-in one is what lets a producer
add a field without breaking this consumer: the unknown field lands in the map
and is ignored.

## Development

```bash
make test    # unit tests, including a parse check of every published .avsc
make vet
make build
```

Requires Go 1.26+. Running the full pipeline requires the monorepo's
`docker compose up`.
