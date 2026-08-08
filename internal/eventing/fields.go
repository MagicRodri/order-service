package eventing

import (
	"encoding/json"
	"fmt"
	"sort"
)

// Avro unions decode to map[string]any keyed by the branch type name, e.g.
// {"string": "abc"} for a ["null","string"] union. Debezium emits optional
// fields that way, so every accessor unwraps it before reading the value.
func unwrap(v any) any {
	m, ok := v.(map[string]any)
	if !ok || len(m) != 1 {
		return v
	}
	for key, inner := range m {
		switch key {
		case "string", "int", "long", "float", "double", "boolean", "bytes":
			return inner
		}
		// A named record branch, e.g. {"io.debezium.connector.Value": {...}}.
		if _, isRecord := inner.(map[string]any); isRecord {
			return inner
		}
	}
	return v
}

func String(rec map[string]any, key string) string {
	v, ok := rec[key]
	if !ok {
		return ""
	}
	switch s := unwrap(v).(type) {
	case string:
		return s
	case []byte:
		return string(s)
	case nil:
		return ""
	default:
		return fmt.Sprint(s)
	}
}

func Int64(rec map[string]any, key string) int64 {
	switch n := unwrap(rec[key]).(type) {
	case int64:
		return n
	case int32:
		return int64(n)
	case int:
		return int64(n)
	case float64:
		return int64(n)
	default:
		return 0
	}
}

func Record(rec map[string]any, key string) (map[string]any, bool) {
	m, ok := unwrap(rec[key]).(map[string]any)
	return m, ok
}

// JSON re-serialises a decoded sub-record so it can be stored in a jsonb column.
func JSON(v any) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	return json.Marshal(v)
}

// FieldNames lists a decoded record's top-level fields, sorted.
//
// It exists so a "missing field" error can report what actually arrived rather
// than only what was expected: an Avro payload shaped differently from the
// contract decodes cleanly and then reads as empty everywhere, which is
// indistinguishable from an empty event until you can see the field names.
func FieldNames(rec map[string]any) []string {
	names := make([]string, 0, len(rec))
	for name := range rec {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
