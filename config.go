// Copyright The Databend Exporter Authors
// SPDX-License-Identifier: Apache-2.0

package databendexporter

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// Config configures the Databend HTTP endpoint and standard Collector helpers.
// See schema.sql for the managed table layout.
type Config struct {
	ClientConfig  confighttp.ClientConfig                                  `mapstructure:",squash"`
	QueueSettings configoptional.Optional[exporterhelper.QueueBatchConfig] `mapstructure:"sending_queue"`
	BackOffConfig configretry.BackOffConfig                                `mapstructure:"retry_on_failure"`
	Username      string                                                   `mapstructure:"username"`
	Password      configopaque.String                                      `mapstructure:"password"`
	CreateSchema  bool                                                     `mapstructure:"create_schema"`
	Database      string                                                   `mapstructure:"database"`
	LogsTable     string                                                   `mapstructure:"logs_table_name"`
	TracesTable   string                                                   `mapstructure:"traces_table_name"`
	MetricsTable  string                                                   `mapstructure:"metrics_table_name"`
}

func createDefaultConfig() *Config {
	httpConfig := confighttp.NewDefaultClientConfig()
	httpConfig.Timeout = 30 * time.Second
	return &Config{
		ClientConfig:  httpConfig,
		QueueSettings: configoptional.Some(exporterhelper.NewDefaultQueueConfig()),
		BackOffConfig: configretry.NewDefaultBackOffConfig(),
		CreateSchema:  true,
		Database:      "default",
		LogsTable:     "otel_logs",
		TracesTable:   "otel_traces",
		MetricsTable:  "otel_metrics",
	}
}

var identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Validate rejects ambiguous endpoint URLs and unsafe SQL identifiers.
func (c *Config) Validate() error {
	u, err := url.Parse(c.ClientConfig.Endpoint)
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		// Do not echo the URL: a misconfigured endpoint may contain credentials.
		return errors.New("endpoint must be an http(s) origin without credentials, path, query or fragment")
	}
	if c.Username == "" && !c.ClientConfig.Auth.HasValue() {
		return errors.New("username or an HTTP auth extension is required")
	}
	if c.Username != "" && c.ClientConfig.Auth.HasValue() {
		return errors.New("username and an HTTP auth extension are mutually exclusive")
	}
	if strings.Contains(c.Username, ":") {
		return errors.New("username must not contain a colon")
	}
	for _, item := range []struct{ name, value string }{
		{"database", c.Database}, {"logs_table_name", c.LogsTable},
		{"traces_table_name", c.TracesTable}, {"metrics_table_name", c.MetricsTable},
	} {
		if !identifier.MatchString(item.value) {
			return fmt.Errorf("%s must be an ASCII SQL identifier", item.name)
		}
	}
	if c.LogsTable == c.TracesTable || c.LogsTable == c.MetricsTable || c.TracesTable == c.MetricsTable {
		return errors.New("signal table names must be distinct")
	}
	if c.ClientConfig.Timeout <= 0 {
		return errors.New("timeout must be positive")
	}
	// Reserved protocol headers must not override the destination or credentials.
	for _, header := range c.ClientConfig.Headers {
		name := header.Name
		switch strings.ToLower(name) {
		case "authorization", "content-type", "x-databend-sql", "host", "content-length":
			return fmt.Errorf("header %s is reserved", name)
		}
	}
	return nil
}
