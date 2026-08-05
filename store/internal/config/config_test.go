package config

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(mapEnv(map[string]string{
		"WIDE_EVENTS_DATABASE":     "/data/events.duckdb",
		"WIDE_EVENTS_SERVICE":      "billing",
		"WIDE_EVENTS_ENVIRONMENT":  "production",
		"WIDE_EVENTS_INGEST_TOKEN": strings.Repeat("a", 64),
		"WIDE_EVENTS_QUERY_TOKEN":  strings.Repeat("b", 64),
	}))
	require.NoError(t, err)
	assert.Equal(t, "0.0.0.0:7421", cfg.Listen)
	assert.Equal(t, "/run/wide-events/admin.sock", cfg.AdminSocket)
	assert.Equal(t, 30, cfg.RetentionDays)
	assert.Equal(t, int64(32<<20), cfg.WriterQueueBytes)
	assert.Equal(t, int64(2<<30), cfg.FreeFloorBytes)
	assert.Equal(t, int64(4<<30), cfg.QueryTempBytes)
	assert.Equal(t, 2, cfg.QueryActive)
	assert.Equal(t, 4, cfg.QueryWaiting)
	assert.Equal(t, 15*time.Second, cfg.QueryTimeout)
	assert.Equal(t, 2*time.Second, cfg.QueryWaitTimeout)
}

func TestLoadAcceptsExplicitLimits(t *testing.T) {
	cfg, err := Load(mapEnv(map[string]string{
		"WIDE_EVENTS_DATABASE":           "/data/events.duckdb",
		"WIDE_EVENTS_SERVICE":            "billing",
		"WIDE_EVENTS_ENVIRONMENT":        "production",
		"WIDE_EVENTS_INGEST_TOKEN":       strings.Repeat("a", 64),
		"WIDE_EVENTS_QUERY_TOKEN":        strings.Repeat("b", 64),
		"WIDE_EVENTS_LISTEN":             "127.0.0.1:9000",
		"WIDE_EVENTS_ADMIN_SOCKET":       "/tmp/admin.sock",
		"WIDE_EVENTS_RETENTION_DAYS":     "7",
		"WIDE_EVENTS_WRITER_QUEUE_BYTES": "1024",
		"WIDE_EVENTS_FREE_FLOOR_BYTES":   "2048",
		"WIDE_EVENTS_QUERY_TEMP_BYTES":   "4096",
		"WIDE_EVENTS_QUERY_ACTIVE":       "2",
		"WIDE_EVENTS_QUERY_WAITING":      "4",
		"WIDE_EVENTS_QUERY_TIMEOUT":      "15s",
		"WIDE_EVENTS_QUERY_WAIT_TIMEOUT": "2s",
	}))
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:9000", cfg.Listen)
	assert.Equal(t, "/tmp/admin.sock", cfg.AdminSocket)
	assert.Equal(t, 7, cfg.RetentionDays)
	assert.Equal(t, int64(1024), cfg.WriterQueueBytes)
	assert.Equal(t, int64(2048), cfg.FreeFloorBytes)
	assert.Equal(t, int64(4096), cfg.QueryTempBytes)
	assert.Equal(t, 2, cfg.QueryActive)
	assert.Equal(t, 4, cfg.QueryWaiting)
	assert.Equal(t, 15*time.Second, cfg.QueryTimeout)
	assert.Equal(t, 2*time.Second, cfg.QueryWaitTimeout)
}

func TestLoadRejectsQueryLimitOverrides(t *testing.T) {
	valid := map[string]string{
		"WIDE_EVENTS_DATABASE":     "/data/events.duckdb",
		"WIDE_EVENTS_SERVICE":      "billing",
		"WIDE_EVENTS_ENVIRONMENT":  "production",
		"WIDE_EVENTS_INGEST_TOKEN": strings.Repeat("a", 64),
		"WIDE_EVENTS_QUERY_TOKEN":  strings.Repeat("b", 64),
	}
	for name, value := range map[string]string{
		"WIDE_EVENTS_QUERY_ACTIVE":       "3",
		"WIDE_EVENTS_QUERY_WAITING":      "5",
		"WIDE_EVENTS_QUERY_TIMEOUT":      "16s",
		"WIDE_EVENTS_QUERY_WAIT_TIMEOUT": "3s",
	} {
		t.Run(name, func(t *testing.T) {
			env := clone(valid)
			env[name] = value
			_, err := Load(mapEnv(env))
			assert.Error(t, err)
		})
	}
}

func TestLoadRejectsRequiredAndUnsafeSettings(t *testing.T) {
	valid := map[string]string{
		"WIDE_EVENTS_DATABASE":     "/data/events.duckdb",
		"WIDE_EVENTS_SERVICE":      "billing",
		"WIDE_EVENTS_ENVIRONMENT":  "production",
		"WIDE_EVENTS_INGEST_TOKEN": strings.Repeat("a", 64),
		"WIDE_EVENTS_QUERY_TOKEN":  strings.Repeat("b", 64),
	}

	for name, change := range map[string]func(map[string]string){
		"missing database":           func(env map[string]string) { delete(env, "WIDE_EVENTS_DATABASE") },
		"missing service":            func(env map[string]string) { delete(env, "WIDE_EVENTS_SERVICE") },
		"missing environment":        func(env map[string]string) { delete(env, "WIDE_EVENTS_ENVIRONMENT") },
		"missing ingest token":       func(env map[string]string) { delete(env, "WIDE_EVENTS_INGEST_TOKEN") },
		"missing query token":        func(env map[string]string) { delete(env, "WIDE_EVENTS_QUERY_TOKEN") },
		"identical tokens":           func(env map[string]string) { env["WIDE_EVENTS_QUERY_TOKEN"] = env["WIDE_EVENTS_INGEST_TOKEN"] },
		"short token":                func(env map[string]string) { env["WIDE_EVENTS_INGEST_TOKEN"] = strings.Repeat("a", 63) },
		"non hexadecimal token":      func(env map[string]string) { env["WIDE_EVENTS_INGEST_TOKEN"] = strings.Repeat("g", 64) },
		"zero retention":             func(env map[string]string) { env["WIDE_EVENTS_RETENTION_DAYS"] = "0" },
		"zero writer queue":          func(env map[string]string) { env["WIDE_EVENTS_WRITER_QUEUE_BYTES"] = "0" },
		"zero free floor":            func(env map[string]string) { env["WIDE_EVENTS_FREE_FLOOR_BYTES"] = "0" },
		"zero query temporary space": func(env map[string]string) { env["WIDE_EVENTS_QUERY_TEMP_BYTES"] = "0" },
		"zero active queries":        func(env map[string]string) { env["WIDE_EVENTS_QUERY_ACTIVE"] = "0" },
		"zero waiting queries":       func(env map[string]string) { env["WIDE_EVENTS_QUERY_WAITING"] = "0" },
		"zero query timeout":         func(env map[string]string) { env["WIDE_EVENTS_QUERY_TIMEOUT"] = "0s" },
		"zero query wait timeout":    func(env map[string]string) { env["WIDE_EVENTS_QUERY_WAIT_TIMEOUT"] = "0s" },
	} {
		t.Run(name, func(t *testing.T) {
			env := clone(valid)
			change(env)
			_, err := Load(mapEnv(env))
			assert.Error(t, err)
		})
	}
}

func mapEnv(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

func clone(values map[string]string) map[string]string {
	copy := make(map[string]string, len(values))
	for key, value := range values {
		copy[key] = value
	}
	return copy
}
