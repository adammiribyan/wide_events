// Package protocol validates and normalizes the v1 ingestion envelope.
package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	ProtocolVersion     = 1
	MaxEvents           = 100
	MaxAttributes       = 256
	MaxEventBytes       = 64 << 10
	MaxBatchBytes       = 1 << 20
	MaxDecodedBodyBytes = 2 << 20
)

var (
	ErrInvalidJSON         = errors.New("invalid JSON")
	ErrBodyTooLarge        = errors.New("decoded request body exceeds limit")
	ErrTrailingJSON        = errors.New("trailing JSON content")
	ErrUnsupportedProtocol = errors.New("unsupported protocol version")
	ErrInvalidBatch        = errors.New("invalid batch")
	ErrInvalidEvent        = errors.New("invalid event")
	ErrInvalidAttribute    = errors.New("invalid attribute")
	ErrTooManyEvents       = errors.New("too many events")
	ErrTooManyAttributes   = errors.New("too many attributes")
	ErrEventTooLarge       = errors.New("event exceeds size limit")
	ErrBatchTooLarge       = errors.New("batch exceeds size limit")
	ErrInvalidLossReport   = errors.New("invalid loss report")
)

// ValidationError describes an invalid externally supplied protocol value.
// Callers can use errors.Is and errors.As to classify it without inspecting text.
type ValidationError struct {
	Field string
	Err   error
}

func (err *ValidationError) Error() string {
	if err.Field == "" {
		return err.Err.Error()
	}
	return fmt.Sprintf("%s: %v", err.Field, err.Err)
}

func (err *ValidationError) Unwrap() error { return err.Err }

// Batch is the complete unit of idempotent ingestion.
type Batch struct {
	ProtocolVersion             int     `json:"protocol_version"`
	BatchID                     string  `json:"batch_id"`
	Service                     string  `json:"service"`
	Environment                 string  `json:"environment"`
	DroppedEventsSinceLastBatch uint64  `json:"dropped_events_since_last_batch"`
	LossReportID                *string `json:"loss_report_id"`
	Events                      []Event `json:"events"`
}

// Event is one completed request or job in protocol version one.
type Event struct {
	ID         string         `json:"id"`
	OccurredAt time.Time      `json:"occurred_at"`
	Kind       string         `json:"kind"`
	DurationMS *float64       `json:"duration_ms"`
	Deployment *string        `json:"deployment,omitempty"`
	Source     *string        `json:"source,omitempty"`
	Attributes map[string]any `json:"attributes"`
}

// Ack confirms an ingested batch after its transaction commits.
type Ack struct {
	ProtocolVersion int      `json:"protocol_version"`
	BatchID         string   `json:"batch_id"`
	Duplicate       bool     `json:"duplicate"`
	Warnings        []string `json:"warnings"`
}

// DecodeBatch parses one JSON document, validates every v1 invariant, and
// returns the normalized JSON charged to the writer queue.
func DecodeBatch(reader io.Reader) (Batch, []byte, error) {
	limited := &countingReader{reader: io.LimitReader(reader, MaxDecodedBodyBytes+1)}
	decoder := json.NewDecoder(limited)
	decoder.UseNumber()

	var batch Batch
	if err := decoder.Decode(&batch); err != nil {
		if limited.read > MaxDecodedBodyBytes {
			return Batch{}, nil, validation("body", ErrBodyTooLarge)
		}
		return Batch{}, nil, validation("body", ErrInvalidJSON)
	}

	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return Batch{}, nil, validation("body", ErrTrailingJSON)
		}
		if limited.read > MaxDecodedBodyBytes {
			return Batch{}, nil, validation("body", ErrBodyTooLarge)
		}
		return Batch{}, nil, validation("body", ErrInvalidJSON)
	}
	if limited.read > MaxDecodedBodyBytes {
		return Batch{}, nil, validation("body", ErrBodyTooLarge)
	}

	if err := validateBatch(&batch); err != nil {
		return Batch{}, nil, err
	}
	canonical, err := json.Marshal(batch)
	if err != nil {
		return Batch{}, nil, fmt.Errorf("marshal canonical batch: %w", err)
	}
	if len(canonical) > MaxBatchBytes {
		return Batch{}, nil, validation("batch", ErrBatchTooLarge)
	}
	return batch, canonical, nil
}

func validateBatch(batch *Batch) error {
	if batch.ProtocolVersion != ProtocolVersion {
		return validation("protocol_version", ErrUnsupportedProtocol)
	}
	if !validUUID(batch.BatchID) {
		return validation("batch_id", ErrInvalidBatch)
	}
	if strings.TrimSpace(batch.Service) == "" {
		return validation("service", ErrInvalidBatch)
	}
	if strings.TrimSpace(batch.Environment) == "" {
		return validation("environment", ErrInvalidBatch)
	}
	if len(batch.Events) == 0 {
		return validation("events", ErrInvalidBatch)
	}
	if len(batch.Events) > MaxEvents {
		return validation("events", ErrTooManyEvents)
	}
	if batch.DroppedEventsSinceLastBatch == 0 {
		if batch.LossReportID != nil {
			return validation("loss_report_id", ErrInvalidLossReport)
		}
	} else if batch.LossReportID == nil || !validUUID(*batch.LossReportID) {
		return validation("loss_report_id", ErrInvalidLossReport)
	}
	for index := range batch.Events {
		if err := validateEvent(&batch.Events[index]); err != nil {
			return err
		}
	}
	return nil
}

func validateEvent(event *Event) error {
	if !validUUID(event.ID) {
		return validation("event.id", ErrInvalidEvent)
	}
	if event.OccurredAt.IsZero() {
		return validation("event.occurred_at", ErrInvalidEvent)
	}
	if event.Kind != "request" && event.Kind != "job" {
		return validation("event.kind", ErrInvalidEvent)
	}
	if event.DurationMS == nil || math.IsNaN(*event.DurationMS) || math.IsInf(*event.DurationMS, 0) {
		return validation("event.duration_ms", ErrInvalidEvent)
	}
	if len(event.Attributes) > MaxAttributes {
		return validation("event.attributes", ErrTooManyAttributes)
	}
	for key, value := range event.Attributes {
		primitive, err := normalizeAttribute(value)
		if err != nil {
			return validation("event.attributes."+key, ErrInvalidAttribute)
		}
		event.Attributes[key] = primitive
	}
	canonical, err := json.Marshal(event)
	if err != nil {
		return validation("event", ErrInvalidEvent)
	}
	if len(canonical) > MaxEventBytes {
		return validation("event", ErrEventTooLarge)
	}
	return nil
}

func normalizeAttribute(value any) (any, error) {
	switch typed := value.(type) {
	case nil, string, bool:
		return typed, nil
	case json.Number:
		parsed, err := typed.Float64()
		if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
			return nil, ErrInvalidAttribute
		}
		return parsed, nil
	case float64:
		return typed, nil
	default:
		return nil, ErrInvalidAttribute
	}
}

func validUUID(value string) bool {
	_, err := uuid.Parse(value)
	return err == nil
}

func validation(field string, err error) error {
	return &ValidationError{Field: field, Err: err}
}

type countingReader struct {
	reader io.Reader
	read   int
}

func (reader *countingReader) Read(buffer []byte) (int, error) {
	count, err := reader.reader.Read(buffer)
	reader.read += count
	return count, err
}
