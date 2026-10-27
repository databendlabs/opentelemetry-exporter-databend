// Copyright The Databend Exporter Authors
// SPDX-License-Identifier: Apache-2.0

package databendexporter

import (
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func testConfig(endpoint string) *Config {
	c := createDefaultConfig()
	c.ClientConfig.Endpoint, c.Username, c.Password = endpoint, "databend", "secret"
	c.QueueSettings = configoptional.None[exporterhelper.QueueBatchConfig]()
	c.BackOffConfig.Enabled = false
	c.CreateSchema = false
	return c
}

func newTestExporter(t *testing.T, c *Config) *databendExporter {
	t.Helper()
	e := &databendExporter{cfg: c, settings: componenttest.NewNopTelemetrySettings()}
	require.NoError(t, e.start(t.Context(), componenttest.NewNopHost()))
	t.Cleanup(func() { require.NoError(t, e.shutdown(context.Background())) })
	return e
}

func TestStreamingLoad(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		require.Equal(t, http.MethodPut, r.Method)
		require.Equal(t, "/v1/streaming_load", r.URL.Path)
		u, p, ok := r.BasicAuth()
		require.True(t, ok)
		require.Equal(t, "databend", u)
		require.Equal(t, "secret", p)
		require.Contains(t, r.Header.Get("X-Databend-SQL"), "`default`.`otel_logs`")
		require.Contains(t, r.Header.Get("X-Databend-SQL"), "FILE_FORMAT = (TYPE = NDJSON)")
		reader, err := r.MultipartReader()
		require.NoError(t, err)
		part, err := reader.NextPart()
		require.NoError(t, err)
		require.Equal(t, "telemetry.ndjson", part.FileName())
		b, err := io.ReadAll(part)
		require.NoError(t, err)
		rows := decodeRows(t, b)
		require.Len(t, rows, 2)
		require.Equal(t, uint64(1700000000123456789), rows[0].TimeUnixNano)
		_, err = reader.NextPart()
		require.ErrorIs(t, err, io.EOF)
		fmt.Fprint(w, `{"id":"query-id","stats":{"rows":2}}`)
	}))
	defer server.Close()
	e := newTestExporter(t, testConfig(server.URL))
	logs, err := (&plog.JSONUnmarshaler{}).UnmarshalLogs([]byte(logsFixture))
	require.NoError(t, err)
	require.NoError(t, e.pushLogs(t.Context(), logs))
	require.NoError(t, e.pushLogs(t.Context(), plog.NewLogs()))
	require.NoError(t, e.pushTraces(t.Context(), ptrace.NewTraces()))
	require.NoError(t, e.pushMetrics(t.Context(), pmetric.NewMetrics()))
	require.Equal(t, int32(1), calls.Load())
}

func TestLoadErrors(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		body      string
		permanent bool
	}{
		{"bad request", 400, "sensitive SQL and telemetry", true},
		{"unauthorized", 401, "secret", true},
		{"forbidden", 403, "secret", true},
		{"redirect", 307, "", true},
		{"timeout", 408, "", false},
		{"rate limit", 429, "", false},
		{"server error", 500, "", false},
		{"unavailable", 503, "", false},
		{"malformed ack", 200, "not JSON", false},
		{"oversized ack", 200, strings.Repeat(" ", (64<<10)+1), false},
		{"trailing ack", 200, `{"id":"x","stats":{"rows":1}} {}`, false},
		{"empty ack", 200, "{}", false},
		{"missing stats", 200, `{"id":"x"}`, false},
		{"wrong count", 200, `{"id":"x","stats":{"rows":2}}`, false},
		{"server error ack", 200, `{"error":{"message":"sensitive"}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				fmt.Fprint(w, tc.body)
			}))
			defer server.Close()
			e := newTestExporter(t, testConfig(server.URL))
			err := e.load(t.Context(), []byte("{}\n"), 1, "otel_logs")
			require.Error(t, err)
			require.Equal(t, tc.permanent, consumererror.IsPermanent(err))
			require.NotContains(t, err.Error(), "secret")
			require.NotContains(t, err.Error(), "sensitive")
		})
	}
}

func TestCancellationAndTransportError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()
	e := newTestExporter(t, testConfig(server.URL))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, e.load(ctx, []byte("{}\n"), 1, "otel_logs"), context.Canceled)
	server.Close()
	err := e.load(t.Context(), []byte("{}\n"), 1, "otel_logs")
	require.ErrorContains(t, err, "transport failure")
	require.False(t, consumererror.IsPermanent(err))
	require.NotContains(t, err.Error(), server.URL)
}

func TestRedirectDoesNotLeakData(t *testing.T) {
	var received atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { received.Add(1) }))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer source.Close()
	e := newTestExporter(t, testConfig(source.URL))
	require.Error(t, e.load(t.Context(), []byte("{}\n"), 1, "otel_logs"))
	require.Zero(t, received.Load())
}

func TestFactoryAllSignals(t *testing.T) {
	var tables []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sql := r.Header.Get("X-Databend-SQL")
		tables = append(tables, sql)
		count := 1
		if strings.Contains(sql, "otel_logs") {
			count = 2
		}
		if strings.Contains(sql, "otel_metrics") {
			count = 6
		}
		fmt.Fprintf(w, `{"id":"x","stats":{"rows":%d}}`, count)
	}))
	defer server.Close()
	f := NewFactory()
	require.Equal(t, componentType, f.Type())
	require.IsType(t, &Config{}, f.CreateDefaultConfig())
	require.Equal(t, component.StabilityLevelDevelopment, f.LogsStability())
	require.Equal(t, component.StabilityLevelDevelopment, f.TracesStability())
	require.Equal(t, component.StabilityLevelDevelopment, f.MetricsStability())
	set := exportertest.NewNopSettings(componentType)
	ctx := t.Context()
	logs, err := f.CreateLogs(ctx, set, testConfig(server.URL))
	require.NoError(t, err)
	traces, err := f.CreateTraces(ctx, set, testConfig(server.URL))
	require.NoError(t, err)
	metrics, err := f.CreateMetrics(ctx, set, testConfig(server.URL))
	require.NoError(t, err)
	for _, exp := range []component.Component{logs, traces, metrics} {
		require.NoError(t, exp.Start(ctx, componenttest.NewNopHost()))
		t.Cleanup(func() { require.NoError(t, exp.Shutdown(context.Background())) })
	}
	ld, err := (&plog.JSONUnmarshaler{}).UnmarshalLogs([]byte(logsFixture))
	require.NoError(t, err)
	td, err := (&ptrace.JSONUnmarshaler{}).UnmarshalTraces([]byte(tracesFixture))
	require.NoError(t, err)
	md, err := (&pmetric.JSONUnmarshaler{}).UnmarshalMetrics([]byte(metricsFixture))
	require.NoError(t, err)
	require.NoError(t, logs.ConsumeLogs(ctx, ld))
	require.NoError(t, traces.ConsumeTraces(ctx, td))
	require.NoError(t, metrics.ConsumeMetrics(ctx, md))
	require.Len(t, tables, 3)
	require.Contains(t, tables[0], "otel_logs")
	require.Contains(t, tables[1], "otel_traces")
	require.Contains(t, tables[2], "otel_metrics")
}

func TestFactoryRetry(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		fmt.Fprint(w, `{"id":"x","stats":{"rows":2}}`)
	}))
	defer server.Close()
	c := testConfig(server.URL)
	c.BackOffConfig.Enabled = true
	c.BackOffConfig.InitialInterval = time.Millisecond
	c.BackOffConfig.MaxInterval = 2 * time.Millisecond
	c.BackOffConfig.MaxElapsedTime = time.Second
	exp, err := NewFactory().CreateLogs(t.Context(), exportertest.NewNopSettings(componentType), c)
	require.NoError(t, err)
	require.NoError(t, exp.Start(t.Context(), componenttest.NewNopHost()))
	defer exp.Shutdown(context.Background())
	ld, err := (&plog.JSONUnmarshaler{}).UnmarshalLogs([]byte(logsFixture))
	require.NoError(t, err)
	require.NoError(t, exp.ConsumeLogs(t.Context(), ld))
	require.Equal(t, int32(2), calls.Load())
}

func TestTLSVerification(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"id":"x","stats":{"rows":1}}`)
	}))
	defer server.Close()
	c := testConfig(server.URL)
	untrusted := newTestExporter(t, c)
	require.Error(t, untrusted.load(t.Context(), []byte("{}\n"), 1, "otel_logs"))
	c = testConfig(server.URL)
	c.ClientConfig.TLS.CAPem = configopaque.String(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}))
	trusted := newTestExporter(t, c)
	require.NoError(t, trusted.load(t.Context(), []byte("{}\n"), 1, "otel_logs"))
}

func TestStartErrors(t *testing.T) {
	e := &databendExporter{cfg: createDefaultConfig()}
	require.Error(t, e.start(t.Context(), componenttest.NewNopHost()))
	e.cfg = testConfig("https://localhost")
	e.cfg.ClientConfig.TLS.CAFile = "/nonexistent-databend-exporter-ca.pem"
	e.settings = componenttest.NewNopTelemetrySettings()
	require.Error(t, e.start(t.Context(), componenttest.NewNopHost()))
}

func TestResponseBodyTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	c := testConfig(server.URL)
	c.ClientConfig.Timeout = 50 * time.Millisecond
	e := newTestExporter(t, c)
	err := e.load(t.Context(), []byte("{}\n"), 1, "otel_logs")
	require.Error(t, err)
	require.False(t, consumererror.IsPermanent(err))
}

func TestPermanentEncodingError(t *testing.T) {
	e := &databendExporter{}
	err := e.export(t.Context(), nil, errors.New("marshal failed"), "logs", "otel_logs")
	require.True(t, consumererror.IsPermanent(err))
	err = e.export(t.Context(), []byte(`{`), nil, "logs", "otel_logs")
	require.True(t, consumererror.IsPermanent(err))
	require.NoError(t, e.export(t.Context(), []byte(`{}`), nil, "logs", "otel_logs"))
	require.NoError(t, e.shutdown(t.Context()))
}
