// Copyright The Databend Exporter Authors
// SPDX-License-Identifier: Apache-2.0

package databendexporter

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

const logsFixture = `{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"checkout"}},{"key":"nested","value":{"kvlistValue":{"values":[{"key":"list","value":{"arrayValue":{"values":[{"intValue":"9223372036854775807"},{"boolValue":true}]}}}]}}}],"droppedAttributesCount":2},"schemaUrl":"resource-schema","scopeLogs":[{"scope":{"name":"test","version":"1","attributes":[{"key":"raw","value":{"bytesValue":"AAEC"}}],"droppedAttributesCount":1},"schemaUrl":"scope-schema","logRecords":[{"observedTimeUnixNano":"1700000000123456789","severityNumber":17,"severityText":"ERROR","body":{"kvlistValue":{"values":[{"key":"message","value":{"stringValue":"quote ' slash \\ newline\n中文"}}]}},"traceId":"0102030405060708090a0b0c0d0e0f10","spanId":"0102030405060708","flags":1,"droppedAttributesCount":3,"eventName":"failure"},{}]}]}]}`

const tracesFixture = `{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"checkout"}}]},"scopeSpans":[{"scope":{"name":"test"},"spans":[{"traceId":"0102030405060708090a0b0c0d0e0f10","spanId":"0102030405060708","parentSpanId":"1112131415161718","traceState":"vendor=value","flags":257,"name":"GET /cart","kind":2,"startTimeUnixNano":"1700000000123456789","endTimeUnixNano":"1700000000123456799","attributes":[{"key":"http.response.status_code","value":{"intValue":"500"}}],"droppedAttributesCount":1,"events":[{"timeUnixNano":"1700000000123456790","name":"exception","droppedAttributesCount":2}],"droppedEventsCount":3,"links":[{"traceId":"11111111111111111111111111111111","spanId":"2222222222222222","traceState":"a=b","flags":257,"droppedAttributesCount":4}],"droppedLinksCount":5,"status":{"message":"failed","code":2}}]}]}]}`

const metricsFixture = `{"resourceMetrics":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"checkout"}}]},"scopeMetrics":[{"scope":{"name":"test"},"metrics":[
{"name":"gauge","description":"temperature","unit":"C","gauge":{"dataPoints":[{"timeUnixNano":"1700000000123456789","asInt":"9223372036854775807","attributes":[{"key":"host","value":{"stringValue":"a"}}],"flags":1},{"timeUnixNano":"1700000000123456790","asDouble":"NaN"}]}},
{"name":"sum","sum":{"aggregationTemporality":2,"isMonotonic":true,"dataPoints":[{"startTimeUnixNano":"1699999999000000001","timeUnixNano":"1700000000123456789","asInt":"-9223372036854775808","exemplars":[{"timeUnixNano":"1700000000123456700","asInt":"42","traceId":"0102030405060708090a0b0c0d0e0f10","spanId":"0102030405060708"}]}]}},
{"name":"hist","histogram":{"aggregationTemporality":1,"dataPoints":[{"timeUnixNano":"1700000000123456789","count":"3","sum":12.5,"bucketCounts":["1","2"],"explicitBounds":[5],"min":1,"max":10}]}},
{"name":"exphist","exponentialHistogram":{"aggregationTemporality":2,"dataPoints":[{"timeUnixNano":"1700000000123456789","count":"3","sum":4,"scale":-2,"zeroCount":"1","zeroThreshold":0.001,"positive":{"offset":-1,"bucketCounts":["2"]},"negative":{"offset":0,"bucketCounts":[]},"min":0,"max":4}]}},
{"name":"summary","summary":{"dataPoints":[{"timeUnixNano":"1700000000123456789","count":"5","sum":12,"quantileValues":[{"quantile":0.5,"value":2},{"quantile":0.99,"value":4}]}]}},
{"name":"empty","gauge":{}},{}
]}]}]}`

func decodeRows(t *testing.T, data []byte) []row {
	t.Helper()
	var rows []row
	d := json.NewDecoder(bytes.NewReader(data))
	for {
		var r row
		err := d.Decode(&r)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		rows = append(rows, r)
	}
	return rows
}

func TestLogsEncoding(t *testing.T) {
	ld, err := (&plog.JSONUnmarshaler{}).UnmarshalLogs([]byte(logsFixture))
	require.NoError(t, err)
	b, err := (&plog.JSONMarshaler{}).MarshalLogs(ld)
	require.NoError(t, err)
	encoded, count, err := encodeRows(b, "logs")
	require.NoError(t, err)
	require.Equal(t, 2, count)
	rows := decodeRows(t, encoded)
	r := rows[0]
	require.Equal(t, uint64(1700000000123456789), r.TimeUnixNano)
	require.Equal(t, "checkout", r.ServiceName)
	require.Equal(t, "failure", r.Name)
	require.Equal(t, "0102030405060708090a0b0c0d0e0f10", r.TraceID)
	require.Equal(t, "0102030405060708", r.SpanID)
	require.Equal(t, "resource-schema", r.ResourceSchemaURL)
	require.Equal(t, "scope-schema", r.ScopeSchemaURL)
	require.Contains(t, string(r.Resource), "9223372036854775807")
	// Rebuild the original hierarchy from stored columns and require a lossless
	// pdata round trip, including structured bodies and dropped-count metadata.
	restored := `{"resourceLogs":[{"resource":` + string(r.Resource) + `,"schemaUrl":"resource-schema","scopeLogs":[{"scope":` + string(r.Scope) + `,"schemaUrl":"scope-schema","logRecords":[` + string(r.Record) + `,` + string(rows[1].Record) + `]}]}]}`
	ld2, err := (&plog.JSONUnmarshaler{}).UnmarshalLogs([]byte(restored))
	require.NoError(t, err)
	b2, err := (&plog.JSONMarshaler{}).MarshalLogs(ld2)
	require.NoError(t, err)
	require.JSONEq(t, string(b), string(b2))
	require.Zero(t, rows[1].TimeUnixNano)
	require.Empty(t, rows[1].TraceID)
}

func TestTracesEncoding(t *testing.T) {
	td, err := (&ptrace.JSONUnmarshaler{}).UnmarshalTraces([]byte(tracesFixture))
	require.NoError(t, err)
	b, err := (&ptrace.JSONMarshaler{}).MarshalTraces(td)
	require.NoError(t, err)
	encoded, count, err := encodeRows(b, "traces")
	require.NoError(t, err)
	require.Equal(t, 1, count)
	r := decodeRows(t, encoded)[0]
	require.Equal(t, uint64(1700000000123456789), r.TimeUnixNano)
	require.Equal(t, "GET /cart", r.Name)
	var root struct {
		ResourceSpans []struct {
			ScopeSpans []struct {
				Spans []json.RawMessage `json:"spans"`
			} `json:"scopeSpans"`
		} `json:"resourceSpans"`
	}
	require.NoError(t, json.Unmarshal(b, &root))
	require.JSONEq(t, string(root.ResourceSpans[0].ScopeSpans[0].Spans[0]), string(r.Record))
}

func TestMetricsEncoding(t *testing.T) {
	md, err := (&pmetric.JSONUnmarshaler{}).UnmarshalMetrics([]byte(metricsFixture))
	require.NoError(t, err)
	b, err := (&pmetric.JSONMarshaler{}).MarshalMetrics(md)
	require.NoError(t, err)
	encoded, count, err := encodeRows(b, "metrics")
	require.NoError(t, err)
	require.Equal(t, 6, count)
	rows := decodeRows(t, encoded)
	require.Equal(t, []string{"gauge", "gauge", "sum", "histogram", "exponentialHistogram", "summary"}, []string{rows[0].MetricType, rows[1].MetricType, rows[2].MetricType, rows[3].MetricType, rows[4].MetricType, rows[5].MetricType})
	for i, r := range rows {
		wantTime := uint64(1700000000123456789)
		if i == 1 {
			wantTime++
		}
		require.Equal(t, wantTime, r.TimeUnixNano)
		require.Equal(t, "checkout", r.ServiceName)
		// Rehydrate each row using the independent pdata decoder to verify full
		// point metadata, rather than asserting only projected index fields.
		payload := `{"resourceMetrics":[{"scopeMetrics":[{"metrics":[` + string(r.Record) + `]}]}]}`
		single, err := (&pmetric.JSONUnmarshaler{}).UnmarshalMetrics([]byte(payload))
		require.NoError(t, err)
		require.Equal(t, 1, single.DataPointCount())
	}
	require.Contains(t, string(rows[0].Record), `"9223372036854775807"`)
	require.Contains(t, string(rows[1].Record), `"NaN"`)
	var sum struct {
		Sum struct {
			AggregationTemporality int  `json:"aggregationTemporality"`
			IsMonotonic            bool `json:"isMonotonic"`
			DataPoints             []struct {
				AsInt     string            `json:"asInt"`
				Exemplars []json.RawMessage `json:"exemplars"`
			} `json:"dataPoints"`
		} `json:"sum"`
	}
	require.NoError(t, json.Unmarshal(rows[2].Record, &sum))
	require.Equal(t, 2, sum.Sum.AggregationTemporality)
	require.True(t, sum.Sum.IsMonotonic)
	require.Equal(t, "-9223372036854775808", sum.Sum.DataPoints[0].AsInt)
	require.Len(t, sum.Sum.DataPoints[0].Exemplars, 1)
}

func TestEmptyEncoding(t *testing.T) {
	for _, signal := range []string{"logs", "traces", "metrics"} {
		b, n, err := encodeRows([]byte(`{}`), signal)
		require.NoError(t, err)
		require.Zero(t, n)
		require.Empty(t, b)
	}
	_, _, err := encodeRows([]byte(`{`), "logs")
	require.Error(t, err)
	_, err = timestamp(json.RawMessage(`"not-a-time"`))
	require.Error(t, err)
}
