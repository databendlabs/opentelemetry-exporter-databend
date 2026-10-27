// Copyright The Databend Exporter Authors
// SPDX-License-Identifier: Apache-2.0

package databendexporter

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Integration tests are opt-in and only modify a unique, temporary database.
// DATABEND_ENDPOINT=http://127.0.0.1:18900 DATABEND_USERNAME=databend
// DATABEND_PASSWORD=databend go test -run TestIntegration -v .
func TestIntegration(t *testing.T) {
	endpoint := os.Getenv("DATABEND_ENDPOINT")
	if endpoint == "" {
		t.Skip("set DATABEND_ENDPOINT to run against a real Databend")
	}
	c := testConfig(endpoint)
	c.Username = os.Getenv("DATABEND_USERNAME")
	c.Password = configopaque.String(os.Getenv("DATABEND_PASSWORD"))
	c.Database = fmt.Sprintf("otel_exporter_test_%d", time.Now().UnixNano())
	require.NoError(t, c.Validate())
	t.Logf("Databend version: %s", querySQL(t, c, "SELECT version()")[0][0])
	querySQL(t, c, "CREATE DATABASE `"+c.Database+"`")
	t.Cleanup(func() { querySQL(t, c, "DROP DATABASE `"+c.Database+"`") })
	schema, err := os.ReadFile("schema.sql")
	require.NoError(t, err)
	for statement := range strings.SplitSeq(strings.ReplaceAll(string(schema), "default.", "`"+c.Database+"`."), ";") {
		if strings.TrimSpace(statement) != "" {
			querySQL(t, c, statement)
		}
	}
	f := NewFactory()
	set := exportertest.NewNopSettings(componentType)
	ctx := t.Context()
	logs, err := f.CreateLogs(ctx, set, c)
	require.NoError(t, err)
	traces, err := f.CreateTraces(ctx, set, c)
	require.NoError(t, err)
	metrics, err := f.CreateMetrics(ctx, set, c)
	require.NoError(t, err)
	require.NoError(t, logs.Start(ctx, componenttest.NewNopHost()))
	require.NoError(t, traces.Start(ctx, componenttest.NewNopHost()))
	require.NoError(t, metrics.Start(ctx, componenttest.NewNopHost()))
	defer logs.Shutdown(ctx)
	defer traces.Shutdown(ctx)
	defer metrics.Shutdown(ctx)
	ld, err := (&plog.JSONUnmarshaler{}).UnmarshalLogs([]byte(logsFixture))
	require.NoError(t, err)
	td, err := (&ptrace.JSONUnmarshaler{}).UnmarshalTraces([]byte(tracesFixture))
	require.NoError(t, err)
	md, err := (&pmetric.JSONUnmarshaler{}).UnmarshalMetrics([]byte(metricsFixture))
	require.NoError(t, err)
	require.NoError(t, logs.ConsumeLogs(ctx, ld))
	require.NoError(t, traces.ConsumeTraces(ctx, td))
	require.NoError(t, metrics.ConsumeMetrics(ctx, md))
	for _, tc := range []struct{ table, count string }{{"otel_logs", "2"}, {"otel_traces", "1"}, {"otel_metrics", "6"}} {
		require.Equal(t, tc.count, querySQL(t, c, "SELECT count(*) FROM `"+c.Database+"`."+tc.table)[0][0])
	}
	logRows := querySQL(t, c, "SELECT time_unix_nano, service_name, trace_id, record:severityNumber::INT, resource_schema_url, scope_schema_url, record:body:kvlistValue:values[0]:value:stringValue::STRING FROM `"+c.Database+"`.otel_logs WHERE name='failure'")
	require.Equal(t, []string{"1700000000123456789", "checkout", "0102030405060708090a0b0c0d0e0f10", "17", "resource-schema", "scope-schema", "quote ' slash \\ newline\n中文"}, logRows[0])
	spanRows := querySQL(t, c, "SELECT record:endTimeUnixNano::STRING,record:links[0]:droppedAttributesCount::INT, record:status:code::INT FROM `"+c.Database+"`.otel_traces")
	require.Equal(t, []string{"1700000000123456799", "4", "2"}, spanRows[0])
	metricRows := querySQL(t, c, "SELECT record:gauge:dataPoints[0]:asInt::STRING FROM `"+c.Database+"`.otel_metrics WHERE name='gauge' AND time_unix_nano=1700000000123456789")
	require.Equal(t, "9223372036854775807", metricRows[0][0])
	kinds := querySQL(t, c, "SELECT metric_type FROM `"+c.Database+"`.otel_metrics GROUP BY metric_type ORDER BY metric_type")
	require.Equal(t, [][]string{{"exponentialHistogram"}, {"gauge"}, {"histogram"}, {"sum"}, {"summary"}}, kinds)
	// Both finite numbers and OTLP's special floating point strings survive.
	require.Equal(t, "NaN", querySQL(t, c, "SELECT record:gauge:dataPoints[0]:asDouble::STRING FROM `"+c.Database+"`.otel_metrics WHERE name='gauge' AND time_unix_nano=1700000000123456790")[0][0])
	// A missing table is a permanent configuration error, not a successful write.
	e := newTestExporter(t, c)
	err = e.load(ctx, []byte("{}\n"), 1, "missing_table")
	require.Error(t, err)
	require.True(t, consumererror.IsPermanent(err))
	// Valid rows must not commit when a later row cannot be decoded.
	validJSON, err := (&plog.JSONMarshaler{}).MarshalLogs(ld)
	require.NoError(t, err)
	validRows, _, err := encodeRows(validJSON, "logs")
	require.NoError(t, err)
	badBatch := append(validRows, []byte("{\"time_unix_nano\":\"not-a-number\"}\n")...)
	err = e.load(ctx, badBatch, 3, "otel_logs")
	require.Error(t, err)
	require.Equal(t, "2", querySQL(t, c, "SELECT count(*) FROM `"+c.Database+"`.otel_logs")[0][0])
}

func TestIntegrationAutoSchema(t *testing.T) {
	endpoint := os.Getenv("DATABEND_ENDPOINT")
	if endpoint == "" {
		t.Skip("set DATABEND_ENDPOINT")
	}
	c := testConfig(endpoint)
	c.Username = os.Getenv("DATABEND_USERNAME")
	c.Password = configopaque.String(os.Getenv("DATABEND_PASSWORD"))
	c.Database = fmt.Sprintf("otel_auto_schema_test_%d", time.Now().UnixNano())
	c.LogsTable, c.TracesTable, c.MetricsTable = "custom_logs", "custom_spans", "custom_metrics"
	c.CreateSchema = true
	t.Cleanup(func() { querySQL(t, c, "DROP DATABASE IF EXISTS `"+c.Database+"`") })
	factory := NewFactory()
	set := exportertest.NewNopSettings(componentType)
	logs, err := factory.CreateLogs(t.Context(), set, c)
	require.NoError(t, err)
	traces, err := factory.CreateTraces(t.Context(), set, c)
	require.NoError(t, err)
	metrics, err := factory.CreateMetrics(t.Context(), set, c)
	require.NoError(t, err)
	for _, e := range []component.Component{logs, traces, metrics} {
		require.NoError(t, e.Start(t.Context(), componenttest.NewNopHost()))
		defer e.Shutdown(t.Context())
	}
	ld, err := (&plog.JSONUnmarshaler{}).UnmarshalLogs([]byte(logsFixture))
	require.NoError(t, err)
	require.NoError(t, logs.ConsumeLogs(t.Context(), ld))
	require.Equal(t, "2", querySQL(t, c, "SELECT count(*) FROM `"+c.Database+"`.custom_logs")[0][0])
	for _, table := range []string{c.TracesTable, c.MetricsTable} {
		require.Equal(t, "0", querySQL(t, c, "SELECT count(*) FROM `"+c.Database+"`.`"+table+"`")[0][0])
	}
}

// querySQL is test provisioning/verification only.
func querySQL(t *testing.T, c *Config, sql string) [][]string {
	t.Helper()
	payload, err := json.Marshal(map[string]any{"sql": sql, "pagination": map[string]int{"wait_time_secs": 10}})
	require.NoError(t, err)
	method, url, body := http.MethodPost, strings.TrimRight(c.ClientConfig.Endpoint, "/")+"/v1/query", io.Reader(bytes.NewReader(payload))
	client := &http.Client{Timeout: 30 * time.Second}
	var data [][]string
	for page := 0; page < 100; page++ {
		req, err := http.NewRequest(method, url, body)
		require.NoError(t, err)
		req.SetBasicAuth(c.Username, string(c.Password))
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		require.NoError(t, err)
		b, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode, string(b))
		var result struct {
			Data    [][]string      `json:"data"`
			Error   json.RawMessage `json:"error"`
			NextURI string          `json:"next_uri"`
		}
		require.NoError(t, json.Unmarshal(b, &result), string(b))
		require.True(t, len(result.Error) == 0 || string(result.Error) == "null", "query failed: %s", b)
		data = append(data, result.Data...)
		if result.NextURI == "" {
			return data
		}
		method, url, body = http.MethodGet, strings.TrimRight(c.ClientConfig.Endpoint, "/")+result.NextURI, nil
	}
	t.Fatal("Databend query did not finish within 100 pages")
	return nil
}
