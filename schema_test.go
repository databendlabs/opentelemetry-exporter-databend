// Copyright The Databend Exporter Authors
// SPDX-License-Identifier: Apache-2.0

package databendexporter

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
)

func TestAutomaticSchema(t *testing.T) {
	var statements []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "warehouse", r.Header.Get("X-Databend-Route"))
		if r.Method == http.MethodGet {
			require.Equal(t, "node1", r.Header.Get("X-Databend-Node-ID"))
			require.Equal(t, "hint1", r.Header.Get("X-Databend-Route-Hint"))
		}
		if r.Method == http.MethodPost {
			var request struct {
				SQL string `json:"sql"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
			statements = append(statements, request.SQL)
			w.Header().Set("X-Databend-Route-Hint", "hint1")
			fmt.Fprint(w, `{"id":"query1","node_id":"node1","state":"Running","next_uri":"/v1/query/query1/page/0"}`)
		} else if strings.HasSuffix(r.URL.Path, "/page/0") {
			fmt.Fprint(w, `{"id":"query1","state":"Succeeded","next_uri":"/v1/query/query1/final"}`)
		} else {
			require.Equal(t, "/v1/query/query1/final", r.URL.Path)
			fmt.Fprint(w, `{"id":"query1","next_uri":null}`)
		}
	}))
	defer server.Close()
	c := testConfig(server.URL)
	c.CreateSchema = true
	c.Database = "observability"
	c.TracesTable = "my_traces"
	e := &databendExporter{cfg: c, tableName: c.TracesTable, settings: componenttest.NewNopTelemetrySettings()}
	require.NoError(t, e.start(t.Context(), componenttest.NewNopHost()))
	defer e.shutdown(context.Background())
	require.Len(t, statements, 2)
	require.Equal(t, "CREATE DATABASE IF NOT EXISTS `observability`", statements[0])
	require.Contains(t, statements[1], "CREATE TABLE IF NOT EXISTS `observability`.`my_traces`")
	require.Contains(t, statements[1], "record VARIANT")
}

func TestSchemaErrors(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
	}{
		{"forbidden", "secret", 403},
		{"invalid JSON", "secret", 200},
		{"oversized", strings.Repeat(" ", (128<<10)+1), 200},
		{"query error", `{"error":{"message":"secret"}}`, 200},
		{"not succeeded", `{"id":"q"}`, 200},
		{"missing query id", `{"state":"Succeeded"}`, 200},
		{"failed", `{"id":"q","state":"Failed"}`, 200},
		{"remote pagination", `{"id":"q","state":"Running","next_uri":"https://elsewhere.example.com/"}`, 200},
		{"other query", `{"id":"q","next_uri":"/v1/query/other/final"}`, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(tc.status); fmt.Fprint(w, tc.body) }))
			defer server.Close()
			c := testConfig(server.URL)
			c.CreateSchema = true
			e := &databendExporter{cfg: c, tableName: c.LogsTable, settings: componenttest.NewNopTelemetrySettings()}
			err := e.start(t.Context(), componenttest.NewNopHost())
			require.Error(t, err)
			require.NotContains(t, err.Error(), "secret")
		})
	}
}
