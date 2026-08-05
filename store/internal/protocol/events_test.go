package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeBatchCanonicalizesAcceptedBatch(t *testing.T) {
	input := `{"events":[{"attributes":{"count":2,"ok":true,"empty":null,"name":"reports"},"duration_ms":42.7,"kind":"request","occurred_at":"2026-08-05T12:34:56.789Z","id":"018f6f50-2f9d-7fb0-a258-6d9f95f20c02"}],"loss_report_id":null,"dropped_events_since_last_batch":0,"environment":"production","service":"billing","batch_id":"018f6f50-2f9d-7fb0-a258-6d9f95f20c01","protocol_version":1}`

	batch, canonical, err := DecodeBatch(strings.NewReader(input))
	require.NoError(t, err)
	assert.Equal(t, ProtocolVersion, batch.ProtocolVersion)
	assert.Equal(t, "billing", batch.Service)
	assert.Equal(t, "production", batch.Environment)
	require.Len(t, batch.Events, 1)
	assert.Equal(t, float64(2), batch.Events[0].Attributes["count"])
	assert.Equal(t, []byte(`{"protocol_version":1,"batch_id":"018f6f50-2f9d-7fb0-a258-6d9f95f20c01","service":"billing","environment":"production","dropped_events_since_last_batch":0,"loss_report_id":null,"events":[{"id":"018f6f50-2f9d-7fb0-a258-6d9f95f20c02","occurred_at":"2026-08-05T12:34:56.789Z","kind":"request","duration_ms":42.7,"attributes":{"count":2,"empty":null,"name":"reports","ok":true}}]}`), canonical)
}

func TestDecodeBatchRejectsInvalidAttributes(t *testing.T) {
	for name, attribute := range map[string]any{
		"nested object": map[string]any{"not": "flat"},
		"array":         []any{"not", "flat"},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := DecodeBatch(strings.NewReader(batchJSON(map[string]any{"attribute": attribute})))
			assert.ErrorIs(t, err, ErrInvalidAttribute)
			var validation *ValidationError
			assert.True(t, errors.As(err, &validation))
		})
	}
}

func TestDecodeBatchRejectsInvalidEnvelope(t *testing.T) {
	for name, mutate := range map[string]func(map[string]any){
		"unsupported protocol": func(batch map[string]any) { batch["protocol_version"] = 2 },
		"invalid batch id":     func(batch map[string]any) { batch["batch_id"] = "not-a-uuid" },
		"missing service":      func(batch map[string]any) { batch["service"] = "" },
		"missing environment":  func(batch map[string]any) { batch["environment"] = "" },
		"empty events":         func(batch map[string]any) { batch["events"] = []any{} },
		"invalid event id":     func(batch map[string]any) { batch["events"].([]any)[0].(map[string]any)["id"] = "not-a-uuid" },
		"invalid occurred at":  func(batch map[string]any) { batch["events"].([]any)[0].(map[string]any)["occurred_at"] = "not-a-time" },
		"unsupported kind":     func(batch map[string]any) { batch["events"].([]any)[0].(map[string]any)["kind"] = "log" },
		"missing duration":     func(batch map[string]any) { delete(batch["events"].([]any)[0].(map[string]any), "duration_ms") },
		"loss id with no loss": func(batch map[string]any) { batch["loss_report_id"] = "018f6f50-2f9d-7fb0-a258-6d9f95f20c03" },
		"missing loss id":      func(batch map[string]any) { batch["dropped_events_since_last_batch"] = 1 },
		"invalid loss id": func(batch map[string]any) {
			batch["dropped_events_since_last_batch"] = 1
			batch["loss_report_id"] = "not-a-uuid"
		},
	} {
		t.Run(name, func(t *testing.T) {
			batch := validBatch()
			mutate(batch)
			_, _, err := DecodeBatch(strings.NewReader(marshalJSON(t, batch)))
			assert.Error(t, err)
			var validation *ValidationError
			assert.True(t, errors.As(err, &validation))
		})
	}
}

func TestDecodeBatchRejectsTrailingJSON(t *testing.T) {
	_, _, err := DecodeBatch(strings.NewReader(batchJSON(nil) + " {}"))
	assert.Error(t, err)
}

func TestDecodeBatchEnforcesEventAndAttributeBoundaries(t *testing.T) {
	t.Run("events", func(t *testing.T) {
		atLimit := validBatch()
		atLimit["events"] = repeatedEvents(MaxEvents)
		_, _, err := DecodeBatch(strings.NewReader(marshalJSON(t, atLimit)))
		assert.NoError(t, err)

		overLimit := validBatch()
		overLimit["events"] = repeatedEvents(MaxEvents + 1)
		_, _, err = DecodeBatch(strings.NewReader(marshalJSON(t, overLimit)))
		assert.Error(t, err)
	})

	t.Run("attributes", func(t *testing.T) {
		atLimit := map[string]any{}
		for index := 0; index < MaxAttributes; index++ {
			atLimit[fmt.Sprintf("key_%03d", index)] = true
		}
		_, _, err := DecodeBatch(strings.NewReader(batchJSON(atLimit)))
		assert.NoError(t, err)

		atLimit["one_more"] = true
		_, _, err = DecodeBatch(strings.NewReader(batchJSON(atLimit)))
		assert.Error(t, err)
	})
}

func TestDecodeBatchEnforcesCanonicalEventAndDecodedBodyBoundaries(t *testing.T) {
	base := validBatch()
	event := base["events"].([]any)[0].(map[string]any)
	event["attributes"] = map[string]any{"payload": strings.Repeat("x", MaxEventBytes)}
	_, _, err := DecodeBatch(strings.NewReader(marshalJSON(t, base)))
	assert.Error(t, err)

	within := strings.Repeat(" ", MaxDecodedBodyBytes-len(batchJSON(nil))) + batchJSON(nil)
	_, _, err = DecodeBatch(strings.NewReader(within))
	assert.NoError(t, err)

	over := strings.Repeat(" ", MaxDecodedBodyBytes-len(batchJSON(nil))+1) + batchJSON(nil)
	_, _, err = DecodeBatch(strings.NewReader(over))
	assert.ErrorIs(t, err, ErrBodyTooLarge)
}

func TestDecodeBatchRequiresDurationForEachV1Kind(t *testing.T) {
	for _, kind := range []string{"request", "job"} {
		t.Run(kind, func(t *testing.T) {
			batch := validBatch()
			batch["events"].([]any)[0].(map[string]any)["kind"] = kind
			_, _, err := DecodeBatch(strings.NewReader(marshalJSON(t, batch)))
			assert.NoError(t, err)
		})
	}
}

func validBatch() map[string]any {
	return map[string]any{
		"protocol_version":                ProtocolVersion,
		"batch_id":                        "018f6f50-2f9d-7fb0-a258-6d9f95f20c01",
		"service":                         "billing",
		"environment":                     "production",
		"dropped_events_since_last_batch": 0,
		"loss_report_id":                  nil,
		"events": []any{map[string]any{
			"id":          "018f6f50-2f9d-7fb0-a258-6d9f95f20c02",
			"occurred_at": time.Date(2026, 8, 5, 12, 34, 56, 789000000, time.UTC).Format(time.RFC3339Nano),
			"kind":        "request",
			"duration_ms": 42.7,
			"attributes":  map[string]any{"name": "reports"},
		}},
	}
}

func repeatedEvents(count int) []any {
	events := make([]any, count)
	for index := range events {
		event := validBatch()["events"].([]any)[0].(map[string]any)
		event["id"] = fmt.Sprintf("018f6f50-2f9d-7fb0-a258-%012d", index)
		events[index] = event
	}
	return events
}

func batchJSON(attributes map[string]any) string {
	batch := validBatch()
	if attributes != nil {
		batch["events"].([]any)[0].(map[string]any)["attributes"] = attributes
	}
	encoded, _ := json.Marshal(batch)
	return string(encoded)
}

func marshalJSON(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	require.NoError(t, err)
	return string(encoded)
}

func TestDecodeBatchAcceptsExactlyMaximumEventBytes(t *testing.T) {
	// The full event's canonical JSON must be measured, rather than a source field.
	// Find the literal payload length whose canonical event is exactly at the limit.
	for payloadBytes := MaxEventBytes; payloadBytes >= 0; payloadBytes-- {
		batch := validBatch()
		batch["events"].([]any)[0].(map[string]any)["attributes"] = map[string]any{"payload": strings.Repeat("x", payloadBytes)}
		encoded := marshalJSON(t, batch)
		decoded, _, err := DecodeBatch(strings.NewReader(encoded))
		if err != nil {
			continue
		}
		canonicalEvent, err := json.Marshal(decoded.Events[0])
		require.NoError(t, err)
		if len(canonicalEvent) == MaxEventBytes {
			assert.NoError(t, err)
			return
		}
	}
	t.Fatal("could not construct an event exactly at MaxEventBytes")
}

func TestDecodeBatchRejectsOneByteBeyondMaximumEventBytes(t *testing.T) {
	batch := validBatch()
	batch["events"].([]any)[0].(map[string]any)["attributes"] = map[string]any{"payload": strings.Repeat("x", MaxEventBytes)}
	_, _, err := DecodeBatch(strings.NewReader(marshalJSON(t, batch)))
	assert.Error(t, err)
}

func TestDecodeBatchReturnsNoCanonicalBytesOnValidationFailure(t *testing.T) {
	batch, canonical, err := DecodeBatch(bytes.NewBufferString("not json"))
	assert.Error(t, err)
	assert.Empty(t, canonical)
	assert.Equal(t, Batch{}, batch)
}

func TestDecodeBatchClassifiesOversizedMalformedBody(t *testing.T) {
	_, _, err := DecodeBatch(strings.NewReader(strings.Repeat(" ", MaxDecodedBodyBytes+1)))
	assert.ErrorIs(t, err, ErrBodyTooLarge)
}
