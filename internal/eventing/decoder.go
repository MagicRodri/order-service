package eventing

import (
	"context"
	"fmt"
	"sync"

	"github.com/hamba/avro/v2"
	"github.com/twmb/franz-go/pkg/sr"
)

// Decoder turns Confluent-framed Avro payloads into generic maps.
//
// It always decodes with the *writer's* schema, fetched from the registry by
// the ID embedded in the message. That is what keeps a consumer working when a
// producer adds a field: the extra field lands in the map and is ignored,
// rather than breaking the decode.
type Decoder struct {
	registry *sr.Client
	header   sr.ConfluentHeader

	mu     sync.RWMutex
	cached map[int]avro.Schema
}

func NewDecoder(registryURL string) (*Decoder, error) {
	client, err := sr.NewClient(sr.URLs(registryURL))
	if err != nil {
		return nil, fmt.Errorf("schema registry client: %w", err)
	}
	return &Decoder{registry: client, cached: make(map[int]avro.Schema)}, nil
}

// Decode strips the Confluent wire header and decodes the Avro body.
func (d *Decoder) Decode(ctx context.Context, msg []byte) (map[string]any, error) {
	id, body, err := d.header.DecodeID(msg)
	if err != nil {
		return nil, fmt.Errorf("decode confluent header: %w", err)
	}
	schema, err := d.schemaByID(ctx, id)
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	if err := avro.Unmarshal(schema, body, &out); err != nil {
		return nil, fmt.Errorf("avro decode with schema %d: %w", id, err)
	}
	return out, nil
}

func (d *Decoder) schemaByID(ctx context.Context, id int) (avro.Schema, error) {
	d.mu.RLock()
	schema, ok := d.cached[id]
	d.mu.RUnlock()
	if ok {
		return schema, nil
	}

	text, err := d.registry.SchemaTextByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("fetch schema %d: %w", id, err)
	}
	schema, err = avro.Parse(text)
	if err != nil {
		return nil, fmt.Errorf("parse schema %d: %w", id, err)
	}

	d.mu.Lock()
	d.cached[id] = schema
	d.mu.Unlock()
	return schema, nil
}
