// Copyright The Databend Exporter Authors
// SPDX-License-Identifier: Apache-2.0

package databendexporter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type databendExporter struct {
	cfg       *Config
	settings  component.TelemetrySettings
	client    *http.Client
	tableName string
}

func (e *databendExporter) start(ctx context.Context, host component.Host) error {
	if err := e.cfg.Validate(); err != nil {
		return err
	}
	client, err := e.cfg.ClientConfig.ToClient(ctx, host.GetExtensions(), e.settings)
	if err != nil {
		return err
	}
	// Never forward telemetry or credentials to an unexpected redirect target.
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	e.client = client
	if e.cfg.CreateSchema {
		if err := e.createSchema(ctx); err != nil {
			client.CloseIdleConnections()
			return err
		}
	}
	return nil
}

func (e *databendExporter) shutdown(context.Context) error {
	if e.client != nil {
		e.client.CloseIdleConnections()
	}
	return nil
}

func (e *databendExporter) pushLogs(ctx context.Context, data plog.Logs) error {
	if data.LogRecordCount() == 0 {
		return nil
	}
	b, err := (&plog.JSONMarshaler{}).MarshalLogs(data)
	return e.export(ctx, b, err, "logs", e.cfg.LogsTable)
}

func (e *databendExporter) pushTraces(ctx context.Context, data ptrace.Traces) error {
	if data.SpanCount() == 0 {
		return nil
	}
	b, err := (&ptrace.JSONMarshaler{}).MarshalTraces(data)
	return e.export(ctx, b, err, "traces", e.cfg.TracesTable)
}

func (e *databendExporter) pushMetrics(ctx context.Context, data pmetric.Metrics) error {
	if data.DataPointCount() == 0 {
		return nil
	}
	b, err := (&pmetric.JSONMarshaler{}).MarshalMetrics(data)
	return e.export(ctx, b, err, "metrics", e.cfg.MetricsTable)
}

func (e *databendExporter) export(ctx context.Context, data []byte, marshalErr error, signal, table string) error {
	if marshalErr != nil {
		return consumererror.NewPermanent(fmt.Errorf("marshal %s: %w", signal, marshalErr))
	}
	body, count, err := encodeRows(data, signal)
	if err != nil {
		return consumererror.NewPermanent(fmt.Errorf("encode %s: %w", signal, err))
	}
	if count == 0 {
		return nil
	}
	return e.load(ctx, body, count, table)
}

func (e *databendExporter) load(ctx context.Context, ndjson []byte, count int, table string) error {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("upload", "telemetry.ndjson")
	if err != nil {
		return err
	}
	if _, err := part.Write(ndjson); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, strings.TrimRight(e.cfg.ClientConfig.Endpoint, "/")+"/v1/streaming_load", &body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("X-Databend-Route", "warehouse")
	// Identifiers are validated before constructing SQL. Streaming load uses
	// AbortNum(1) internally; unlike COPY, its SQL does not accept ON_ERROR.
	req.Header.Set("X-Databend-SQL", fmt.Sprintf("INSERT INTO `%s`.`%s` FROM @_databend_load FILE_FORMAT = (TYPE = NDJSON)", e.cfg.Database, table))
	if e.cfg.Username != "" {
		req.SetBasicAuth(e.cfg.Username, string(e.cfg.Password))
	}
	resp, err := e.client.Do(req)
	if err != nil {
		// Do not expose URLs, auth headers, telemetry or echoed server errors.
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return errors.New("Databend streaming load transport failure")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		err := fmt.Errorf("Databend streaming load returned HTTP %d", resp.StatusCode)
		if resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			return err
		}
		return consumererror.NewPermanent(err)
	}
	var result struct {
		ID    string `json:"id"`
		Stats struct {
			Rows *uint64 `json:"rows"`
		} `json:"stats"`
		Error json.RawMessage `json:"error"`
	}
	// Invalid/ambiguous acknowledgements may follow a committed insert. Retry
	// rather than silently lose data; this exporter provides at-least-once writes.
	const maxAcknowledgementBytes = 64 << 10
	ack, err := io.ReadAll(io.LimitReader(resp.Body, maxAcknowledgementBytes+1))
	if err != nil || len(ack) > maxAcknowledgementBytes {
		return errors.New("invalid Databend streaming load acknowledgement")
	}
	if err := json.Unmarshal(ack, &result); err != nil {
		return errors.New("invalid Databend streaming load acknowledgement")
	}
	if len(result.Error) > 0 && string(result.Error) != "null" {
		return errors.New("Databend streaming load returned an error acknowledgement")
	}
	if result.ID == "" || result.Stats.Rows == nil || *result.Stats.Rows != uint64(count) {
		return errors.New("Databend streaming load row count acknowledgement mismatch")
	}
	return nil
}
