// Copyright The Databend Exporter Authors
// SPDX-License-Identifier: Apache-2.0

package databendexporter

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/config/configopaque"
)

// This test sends real OTLP/HTTP through a custom Collector process, in addition
// to the factory-level integration test. It does not use a production warehouse.
func TestCollectorEndToEnd(t *testing.T) {
	binary, endpoint := os.Getenv("DATABEND_COLLECTOR_BINARY"), os.Getenv("DATABEND_ENDPOINT")
	if binary == "" || endpoint == "" {
		t.Skip("set DATABEND_COLLECTOR_BINARY and DATABEND_ENDPOINT")
	}
	binary, err := filepath.Abs(binary)
	require.NoError(t, err)
	c := testConfig(endpoint)
	c.Username = os.Getenv("DATABEND_USERNAME")
	c.Password = configopaque.String(os.Getenv("DATABEND_PASSWORD"))
	c.Database = fmt.Sprintf("otel_collector_test_%d", time.Now().UnixNano())
	require.NoError(t, c.Validate())
	querySQL(t, c, "CREATE DATABASE `"+c.Database+"`")
	t.Cleanup(func() { querySQL(t, c, "DROP DATABASE `"+c.Database+"`") })
	schema, err := os.ReadFile("schema.sql")
	require.NoError(t, err)
	for statement := range strings.SplitSeq(strings.ReplaceAll(string(schema), "default.", "`"+c.Database+"`."), ";") {
		if strings.TrimSpace(statement) != "" {
			querySQL(t, c, statement)
		}
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	// JSON is valid Collector configuration; encoding protects credentials from
	// accidental YAML interpolation. Never write this temporary file to the repo.
	config := map[string]any{
		"receivers": map[string]any{"otlp": map[string]any{"protocols": map[string]any{"http": map[string]any{"endpoint": address}}}},
		"exporters": map[string]any{"databend": map[string]any{
			"endpoint": endpoint, "username": c.Username, "password": string(c.Password), "database": c.Database, "create_schema": false,
			"sending_queue": map[string]any{"enabled": false}, "retry_on_failure": map[string]any{"enabled": false},
		}},
		"service": map[string]any{
			"telemetry": map[string]any{"metrics": map[string]any{"level": "none"}},
			"pipelines": map[string]any{
				"logs":    map[string]any{"receivers": []string{"otlp"}, "exporters": []string{"databend"}},
				"traces":  map[string]any{"receivers": []string{"otlp"}, "exporters": []string{"databend"}},
				"metrics": map[string]any{"receivers": []string{"otlp"}, "exporters": []string{"databend"}},
			},
		},
	}
	b, err := json.Marshal(config)
	require.NoError(t, err)
	dir := t.TempDir()
	configPath := filepath.Join(dir, "collector.json")
	require.NoError(t, os.WriteFile(configPath, b, 0600))
	logPath := filepath.Join(dir, "collector.log")
	logFile, err := os.Create(logPath)
	require.NoError(t, err)
	cmd := exec.Command(binary, "--config", configPath)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
		_ = logFile.Close()
		if t.Failed() {
			output, _ := os.ReadFile(logPath)
			t.Logf("Collector output:\n%s", output)
		}
	})
	require.NoError(t, cmd.Start())
	client := &http.Client{Timeout: 5 * time.Second}
	url := "http://" + address
	require.Eventually(t, func() bool {
		resp, err := client.Get(url + "/")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusNotFound
	}, 15*time.Second, 50*time.Millisecond, "Collector OTLP receiver did not start")
	for _, tc := range []struct{ signal, payload string }{{"logs", logsFixture}, {"traces", tracesFixture}, {"metrics", metricsFixture}} {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url+"/v1/"+tc.signal, bytes.NewBufferString(tc.payload))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		require.NoError(t, err)
		result, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode, string(result))
		var ack struct {
			PartialSuccess struct {
				RejectedLogRecords json.Number `json:"rejectedLogRecords"`
				RejectedSpans      json.Number `json:"rejectedSpans"`
				RejectedDataPoints json.Number `json:"rejectedDataPoints"`
				ErrorMessage       string      `json:"errorMessage"`
			} `json:"partialSuccess"`
		}
		require.NoError(t, json.Unmarshal(result, &ack))
		require.Empty(t, ack.PartialSuccess.ErrorMessage)
		for _, n := range []json.Number{ack.PartialSuccess.RejectedLogRecords, ack.PartialSuccess.RejectedSpans, ack.PartialSuccess.RejectedDataPoints} {
			require.True(t, n == "" || n == "0", "OTLP rejected records: %s", result)
		}
	}
	for _, tc := range []struct{ table, count string }{{"otel_logs", "2"}, {"otel_traces", "1"}, {"otel_metrics", "6"}} {
		require.Equal(t, tc.count, querySQL(t, c, "SELECT count(*) FROM `"+c.Database+"`."+tc.table)[0][0])
	}
}
