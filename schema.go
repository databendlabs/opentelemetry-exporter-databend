// Copyright The Databend Exporter Authors
// SPDX-License-Identifier: Apache-2.0

package databendexporter

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
)

// Keep automatic provisioning and the operator-facing schema in sync.
//
//go:embed schema.sql
var schemaSQL string

func (e *databendExporter) createSchema(ctx context.Context) error {
	if !identifier.MatchString(e.tableName) {
		return errors.New("missing signal table for schema creation")
	}
	if err := e.executeDDL(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", e.cfg.Database)); err != nil {
		return err
	}
	ddl, _, _ := strings.Cut(schemaSQL, ";")
	ddl = strings.ReplaceAll(ddl, "default.otel_logs", fmt.Sprintf("`%s`.`%s`", e.cfg.Database, e.tableName))
	return e.executeDDL(ctx, ddl)
}

var queryPath = regexp.MustCompile(`^/v1/query/[A-Za-z0-9_-]+/(final|page/[0-9]+)$`)

// executeDDL handles asynchronous query responses and finalizes their resources.
// It never follows absolute URLs or arbitrary paths supplied in next_uri.
func (e *databendExporter) executeDDL(parent context.Context, sql string) error {
	ctx, cancel := context.WithTimeout(parent, e.cfg.ClientConfig.Timeout)
	defer cancel()
	payload, err := json.Marshal(map[string]any{"sql": sql, "pagination": map[string]int{"wait_time_secs": 1}})
	if err != nil {
		return err
	}
	method, path, body := http.MethodPost, "/v1/query", io.Reader(bytes.NewReader(payload))
	queryID, nodeID, routeHint := "", "", ""
	succeeded := false
	for {
		req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(e.cfg.ClientConfig.Endpoint, "/")+path, body)
		if err != nil {
			return errors.New("invalid Databend schema request")
		}
		if e.cfg.Username != "" {
			req.SetBasicAuth(e.cfg.Username, string(e.cfg.Password))
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Databend-Route", "warehouse")
		if nodeID != "" {
			req.Header.Set("X-Databend-Node-ID", nodeID)
		}
		if routeHint != "" {
			req.Header.Set("X-Databend-Route-Hint", routeHint)
		}
		resp, err := e.client.Do(req)
		if err != nil {
			return errors.New("Databend schema request transport failure")
		}
		if hint := resp.Header.Get("X-Databend-Route-Hint"); hint != "" {
			routeHint = hint
		}
		b, readErr := io.ReadAll(io.LimitReader(resp.Body, (128<<10)+1))
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("Databend schema request returned HTTP %d", resp.StatusCode)
		}
		if readErr != nil || len(b) > 128<<10 {
			return errors.New("invalid Databend schema acknowledgement")
		}
		var result struct {
			ID      string          `json:"id"`
			NodeID  string          `json:"node_id"`
			State   string          `json:"state"`
			Error   json.RawMessage `json:"error"`
			NextURI string          `json:"next_uri"`
		}
		if json.Unmarshal(b, &result) != nil {
			return errors.New("invalid Databend schema acknowledgement")
		}
		if len(result.Error) > 0 && string(result.Error) != "null" {
			return errors.New("Databend schema query failed; check privileges and server query history")
		}
		if queryID == "" {
			if result.ID == "" {
				return errors.New("Databend schema acknowledgement is missing a query ID")
			}
			queryID = result.ID
		}
		if result.NodeID != "" {
			nodeID = result.NodeID
		}
		if result.ID != "" && result.ID != queryID {
			return errors.New("Databend schema query ID changed")
		}
		if result.State == "Succeeded" {
			succeeded = true
		}
		if result.State == "Failed" {
			return errors.New("Databend schema query failed")
		}
		if result.NextURI == "" {
			if !succeeded {
				return errors.New("Databend schema query did not acknowledge success")
			}
			return nil
		}
		if !queryPath.MatchString(result.NextURI) || !strings.HasPrefix(result.NextURI, "/v1/query/"+queryID+"/") {
			return errors.New("invalid Databend schema pagination path")
		}
		method, path, body = http.MethodGet, result.NextURI, nil
	}
}
