// Package config loads the store's process configuration from its environment.
package config

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	defaultListen             = "0.0.0.0:7421"
	defaultAdminSocket        = "/run/wide-events/admin.sock"
	defaultRetentionDays      = 30
	defaultWriterQueueBytes   = int64(32 << 20)
	defaultFreeFloorBytes     = int64(2 << 30)
	defaultQueryTempBytes     = int64(4 << 30)
	defaultQueryActive        = 2
	defaultQueryWaiting       = 4
	defaultQueryTimeout       = 15 * time.Second
	defaultQueryWaitTimeout   = 2 * time.Second
	minimumTokenHexCharacters = 64
)

// Config is the validated runtime configuration for a single store process.
type Config struct {
	Listen, Database, Service, Environment           string
	IngestToken, QueryToken, AdminSocket             string
	RetentionDays, QueryActive, QueryWaiting         int
	WriterQueueBytes, FreeFloorBytes, QueryTempBytes int64
	QueryTimeout, QueryWaitTimeout                   time.Duration
}

// Load reads and validates configuration using getenv. Supplying getenv keeps
// configuration tests independent from the process environment.
func Load(getenv func(string) string) (Config, error) {
	cfg := Config{
		Listen:           valueOr(getenv("WIDE_EVENTS_LISTEN"), defaultListen),
		Database:         strings.TrimSpace(getenv("WIDE_EVENTS_DATABASE")),
		Service:          strings.TrimSpace(getenv("WIDE_EVENTS_SERVICE")),
		Environment:      strings.TrimSpace(getenv("WIDE_EVENTS_ENVIRONMENT")),
		IngestToken:      strings.TrimSpace(getenv("WIDE_EVENTS_INGEST_TOKEN")),
		QueryToken:       strings.TrimSpace(getenv("WIDE_EVENTS_QUERY_TOKEN")),
		AdminSocket:      valueOr(getenv("WIDE_EVENTS_ADMIN_SOCKET"), defaultAdminSocket),
		RetentionDays:    defaultRetentionDays,
		WriterQueueBytes: defaultWriterQueueBytes,
		FreeFloorBytes:   defaultFreeFloorBytes,
		QueryTempBytes:   defaultQueryTempBytes,
		QueryActive:      defaultQueryActive,
		QueryWaiting:     defaultQueryWaiting,
		QueryTimeout:     defaultQueryTimeout,
		QueryWaitTimeout: defaultQueryWaitTimeout,
	}

	if cfg.Database == "" || cfg.Service == "" || cfg.Environment == "" {
		return Config{}, fmt.Errorf("WIDE_EVENTS_DATABASE, WIDE_EVENTS_SERVICE, and WIDE_EVENTS_ENVIRONMENT are required")
	}
	if err := validateToken("WIDE_EVENTS_INGEST_TOKEN", cfg.IngestToken); err != nil {
		return Config{}, err
	}
	if err := validateToken("WIDE_EVENTS_QUERY_TOKEN", cfg.QueryToken); err != nil {
		return Config{}, err
	}
	if cfg.IngestToken == cfg.QueryToken {
		return Config{}, fmt.Errorf("WIDE_EVENTS_INGEST_TOKEN and WIDE_EVENTS_QUERY_TOKEN must differ")
	}

	var err error
	if cfg.RetentionDays, err = positiveInt(getenv, "WIDE_EVENTS_RETENTION_DAYS", cfg.RetentionDays); err != nil {
		return Config{}, err
	}
	if cfg.WriterQueueBytes, err = positiveInt64(getenv, "WIDE_EVENTS_WRITER_QUEUE_BYTES", cfg.WriterQueueBytes); err != nil {
		return Config{}, err
	}
	if cfg.FreeFloorBytes, err = positiveInt64(getenv, "WIDE_EVENTS_FREE_FLOOR_BYTES", cfg.FreeFloorBytes); err != nil {
		return Config{}, err
	}
	if cfg.QueryTempBytes, err = positiveInt64(getenv, "WIDE_EVENTS_QUERY_TEMP_BYTES", cfg.QueryTempBytes); err != nil {
		return Config{}, err
	}
	if cfg.QueryActive, err = positiveInt(getenv, "WIDE_EVENTS_QUERY_ACTIVE", cfg.QueryActive); err != nil {
		return Config{}, err
	}
	if cfg.QueryWaiting, err = positiveInt(getenv, "WIDE_EVENTS_QUERY_WAITING", cfg.QueryWaiting); err != nil {
		return Config{}, err
	}
	if cfg.QueryTimeout, err = positiveDuration(getenv, "WIDE_EVENTS_QUERY_TIMEOUT", cfg.QueryTimeout); err != nil {
		return Config{}, err
	}
	if cfg.QueryWaitTimeout, err = positiveDuration(getenv, "WIDE_EVENTS_QUERY_WAIT_TIMEOUT", cfg.QueryWaitTimeout); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func valueOr(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func validateToken(name, value string) error {
	if len(value) < minimumTokenHexCharacters {
		return fmt.Errorf("%s must contain at least %d hexadecimal characters", name, minimumTokenHexCharacters)
	}
	if _, err := hex.DecodeString(value); err != nil {
		return fmt.Errorf("%s must be hexadecimal: %w", name, err)
	}
	return nil
}

func positiveInt(getenv func(string) string, name string, fallback int) (int, error) {
	value := getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return parsed, nil
}

func positiveInt64(getenv func(string) string, name string, fallback int64) (int64, error) {
	value := getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return parsed, nil
}

func positiveDuration(getenv func(string) string, name string, fallback time.Duration) (time.Duration, error) {
	value := getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", name)
	}
	return parsed, nil
}
