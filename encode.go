// Copyright The Databend Exporter Authors
// SPDX-License-Identifier: Apache-2.0

package databendexporter

import (
	"bytes"
	"encoding/json"
	"strconv"
)

// A row keeps OTLP JSON intact in VARIANT columns, with a small set of indexes.
// Unix timestamps are UInt64 nanoseconds, avoiding Databend TIMESTAMP's microsecond
// precision limit. OTLP's string-encoded 64-bit integers are never float-decoded.
type row struct {
	TimeUnixNano      uint64          `json:"time_unix_nano"`
	ServiceName       string          `json:"service_name"`
	TraceID           string          `json:"trace_id"`
	SpanID            string          `json:"span_id"`
	Name              string          `json:"name"`
	MetricType        string          `json:"metric_type"`
	Resource          json.RawMessage `json:"resource"`
	ResourceSchemaURL string          `json:"resource_schema_url"`
	Scope             json.RawMessage `json:"scope"`
	ScopeSchemaURL    string          `json:"scope_schema_url"`
	Record            json.RawMessage `json:"record"`
}

type object map[string]json.RawMessage

func jsonString(raw json.RawMessage) string {
	var s string
	_ = json.Unmarshal(raw, &s) // These fields are strings in pdata-generated OTLP JSON.
	return s
}

func timestamp(raw json.RawMessage) (uint64, error) {
	if len(raw) == 0 {
		return 0, nil
	}
	s := string(raw)
	if raw[0] == '"' {
		s = jsonString(raw)
	}
	return strconv.ParseUint(s, 10, 64)
}

func serviceName(raw json.RawMessage) string {
	var resource struct {
		Attributes []struct {
			Key   string `json:"key"`
			Value struct {
				StringValue string `json:"stringValue"`
			} `json:"value"`
		} `json:"attributes"`
	}
	if json.Unmarshal(raw, &resource) != nil {
		return ""
	}
	for _, a := range resource.Attributes {
		if a.Key == "service.name" {
			return a.Value.StringValue
		}
	}
	return ""
}

func nonEmptyObject(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage(`{}`)
	}
	return raw
}

// encodeRows accepts only pdata-generated OTLP JSON. Each log, span or metric
// data point becomes one NDJSON row. Metric metadata is repeated per point so
// temporality, monotonicity, exemplars and optional fields remain self-contained.
func encodeRows(data []byte, signal string) ([]byte, int, error) {
	resourceKey, scopeKey, recordsKey := "resourceLogs", "scopeLogs", "logRecords"
	if signal == "traces" {
		resourceKey, scopeKey, recordsKey = "resourceSpans", "scopeSpans", "spans"
	}
	if signal == "metrics" {
		resourceKey, scopeKey, recordsKey = "resourceMetrics", "scopeMetrics", "metrics"
	}
	var root map[string][]object
	if err := json.Unmarshal(data, &root); err != nil {
		return nil, 0, err
	}
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	count := 0
	for _, resource := range root[resourceKey] {
		if len(resource[scopeKey]) == 0 {
			continue
		}
		var scopes []object
		if err := json.Unmarshal(resource[scopeKey], &scopes); err != nil {
			return nil, 0, err
		}
		base := row{
			Resource:          nonEmptyObject(resource["resource"]),
			ResourceSchemaURL: jsonString(resource["schemaUrl"]),
			ServiceName:       serviceName(resource["resource"]),
		}
		for _, scope := range scopes {
			if len(scope[recordsKey]) == 0 {
				continue
			}
			base.Scope = nonEmptyObject(scope["scope"])
			base.ScopeSchemaURL = jsonString(scope["schemaUrl"])
			var records []json.RawMessage
			if err := json.Unmarshal(scope[recordsKey], &records); err != nil {
				return nil, 0, err
			}
			for _, record := range records {
				rows, err := recordRows(base, record, signal)
				if err != nil {
					return nil, 0, err
				}
				for _, r := range rows {
					if err := encoder.Encode(r); err != nil {
						return nil, 0, err
					}
					count++
				}
			}
		}
	}
	return buf.Bytes(), count, nil
}

func recordRows(base row, record json.RawMessage, signal string) ([]row, error) {
	var fields object
	if err := json.Unmarshal(record, &fields); err != nil {
		return nil, err
	}
	base.Record = record
	base.TraceID = jsonString(fields["traceId"])
	base.SpanID = jsonString(fields["spanId"])
	base.Name = jsonString(fields["name"])
	timeField := fields["timeUnixNano"]
	if signal == "logs" {
		base.Name = jsonString(fields["eventName"])
		t, err := timestamp(timeField)
		if err != nil {
			return nil, err
		}
		if t == 0 {
			timeField = fields["observedTimeUnixNano"]
		}
	}
	if signal == "traces" {
		timeField = fields["startTimeUnixNano"]
	}
	if signal != "metrics" {
		var err error
		base.TimeUnixNano, err = timestamp(timeField)
		return []row{base}, err
	}
	for _, kind := range []string{"gauge", "sum", "histogram", "exponentialHistogram", "summary"} {
		if len(fields[kind]) == 0 {
			continue
		}
		var metric object
		if err := json.Unmarshal(fields[kind], &metric); err != nil {
			return nil, err
		}
		if len(metric["dataPoints"]) == 0 {
			return nil, nil
		}
		var points []json.RawMessage
		if err := json.Unmarshal(metric["dataPoints"], &points); err != nil {
			return nil, err
		}
		rows := make([]row, 0, len(points))
		for _, point := range points {
			var p object
			if err := json.Unmarshal(point, &p); err != nil {
				return nil, err
			}
			r := base
			r.MetricType = kind
			var err error
			r.TimeUnixNano, err = timestamp(p["timeUnixNano"])
			if err != nil {
				return nil, err
			}
			metric["dataPoints"], err = json.Marshal([]json.RawMessage{point})
			if err != nil {
				return nil, err
			}
			fields[kind], err = json.Marshal(metric)
			if err != nil {
				return nil, err
			}
			r.Record, err = json.Marshal(fields)
			if err != nil {
				return nil, err
			}
			rows = append(rows, r)
		}
		return rows, nil
	}
	// An unset metric contains no data points and therefore has no rows.
	return nil, nil
}
