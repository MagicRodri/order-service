package eventing

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hamba/avro/v2"
)

// Every published contract must parse as Avro, or the connector would reject it
// at registration time rather than here.
func TestPublishedSchemasParse(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "schemas", "*.avsc"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("no schemas found: %v", err)
	}
	for _, path := range paths {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		if _, err := avro.Parse(string(body)); err != nil {
			t.Errorf("%s does not parse: %v", filepath.Base(path), err)
		}
	}
}

// Decoding a real Avro payload exercises the same path the consumer uses, so
// the accessors are checked against hamba's actual output types rather than
// hand-built maps.
func TestAccessorsOnDecodedRecord(t *testing.T) {
	body, err := os.ReadFile(filepath.Join("..", "..", "schemas", "OrderCreated.avsc"))
	if err != nil {
		t.Fatal(err)
	}
	schema, err := avro.Parse(string(body))
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := avro.Marshal(schema, map[string]any{
		"event_id":       "8f1d9e0a-0000-4000-8000-000000000001",
		"event_type":     "OrderCreated",
		"occurred_at":    "2026-01-01T00:00:00Z",
		"order_id":       "o-1",
		"customer_id":    "c-1",
		"subtotal_cents": int64(10_000),
		"discount_cents": int64(500),
		"total_cents":    int64(9_500),
		"currency":       "EUR",
		"customer_tier":  "GOLD",
		"item_count":     2,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	decoded := map[string]any{}
	if err := avro.Unmarshal(schema, encoded, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got := String(decoded, "customer_id"); got != "c-1" {
		t.Errorf("customer_id = %q, want c-1", got)
	}
	if got := Int64(decoded, "total_cents"); got != 9_500 {
		t.Errorf("total_cents = %d, want 9500", got)
	}
	if got := Int64(decoded, "item_count"); got != 2 {
		t.Errorf("item_count = %d, want 2", got)
	}
	if got := String(decoded, "absent_field"); got != "" {
		t.Errorf("absent field = %q, want empty", got)
	}
}

// Debezium models optional columns as Avro unions, which decode to a
// single-key map. The accessors must see through that wrapper.
func TestAccessorsUnwrapUnions(t *testing.T) {
	record := map[string]any{
		"reason": map[string]any{"string": "fraud"},
		"amount": map[string]any{"long": int64(42)},
		"absent": nil,
	}
	if got := String(record, "reason"); got != "fraud" {
		t.Errorf("reason = %q, want fraud", got)
	}
	if got := Int64(record, "amount"); got != 42 {
		t.Errorf("amount = %d, want 42", got)
	}
	if got := String(record, "absent"); got != "" {
		t.Errorf("absent = %q, want empty", got)
	}
}

func TestEventTypePrefersHeader(t *testing.T) {
	msg := Message{
		Headers: map[string]string{"eventType": "OrderCancelled"},
		Value:   map[string]any{"event_type": "OrderCreated"},
	}
	if got := msg.EventType(); got != "OrderCancelled" {
		t.Errorf("EventType() = %q, want OrderCancelled", got)
	}

	msg.Headers = nil
	if got := msg.EventType(); got != "OrderCreated" {
		t.Errorf("EventType() fallback = %q, want OrderCreated", got)
	}
}
