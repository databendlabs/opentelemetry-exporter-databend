// Copyright The Databend Exporter Authors
// SPDX-License-Identifier: Apache-2.0

package databendexporter

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/config/configauth"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/confmap"
)

func TestConfigValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Config)
	}{
		{"empty endpoint", func(c *Config) { c.ClientConfig.Endpoint = "" }},
		{"scheme", func(c *Config) { c.ClientConfig.Endpoint = "tcp://localhost:9000" }},
		{"path", func(c *Config) { c.ClientConfig.Endpoint = "http://localhost/v1/query" }},
		{"credentials", func(c *Config) { c.ClientConfig.Endpoint = "http://user:secret@localhost" }},
		{"query", func(c *Config) { c.ClientConfig.Endpoint = "http://localhost?password=secret" }},
		{"fragment", func(c *Config) { c.ClientConfig.Endpoint = "http://localhost/#secret" }},
		{"parse error", func(c *Config) { c.ClientConfig.Endpoint = "http://%" }},
		{"missing username", func(c *Config) { c.Username = "" }},
		{"colon username", func(c *Config) { c.Username = "a:b" }},
		{"ambiguous auth", func(c *Config) { c.ClientConfig.Auth = configoptional.Some(configauth.Config{}) }},
		{"database injection", func(c *Config) { c.Database = "default`;DROP TABLE x;--" }},
		{"table injection", func(c *Config) { c.LogsTable = "logs\r\nx: y" }},
		{"qualified table", func(c *Config) { c.TracesTable = "db.table" }},
		{"empty table", func(c *Config) { c.MetricsTable = "" }},
		{"same table", func(c *Config) { c.MetricsTable = c.LogsTable }},
		{"no timeout", func(c *Config) { c.ClientConfig.Timeout = 0 }},
		{"negative timeout", func(c *Config) { c.ClientConfig.Timeout = -1 }},
		{"sql header", func(c *Config) {
			c.ClientConfig.Headers = configopaque.MapList{{Name: "x-databend-sql", Value: "secret"}}
		}},
		{"auth header", func(c *Config) {
			c.ClientConfig.Headers = configopaque.MapList{{Name: "Authorization", Value: "secret"}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := testConfig("https://localhost")
			tc.change(c)
			err := c.Validate()
			require.Error(t, err)
			require.NotContains(t, err.Error(), "secret")
		})
	}
	for _, endpoint := range []string{"http://localhost:8000", "https://warehouse.example.com/", "http://[::1]:8000"} {
		require.NoError(t, testConfig(endpoint).Validate())
	}
	c := testConfig("https://localhost")
	c.Username = ""
	c.ClientConfig.Auth = configoptional.Some(configauth.Config{})
	require.NoError(t, c.Validate())
	c.ClientConfig.Headers = configopaque.MapList{{Name: "X-Databend-Warehouse", Value: "test"}}
	require.NoError(t, c.Validate())
}

func TestConfigUnmarshal(t *testing.T) {
	c := createDefaultConfig()
	conf := confmap.NewFromStringMap(map[string]any{
		"endpoint":         "https://localhost",
		"username":         "root",
		"password":         "secret",
		"database":         "observability",
		"logs_table_name":  "logs",
		"timeout":          "15s",
		"sending_queue":    map[string]any{"enabled": false},
		"retry_on_failure": map[string]any{"enabled": false},
		"headers":          map[string]any{"X-Databend-Warehouse": "my_wh"},
	})
	require.NoError(t, conf.Unmarshal(c))
	require.NoError(t, c.Validate())
	require.Equal(t, "observability", c.Database)
	require.Equal(t, "logs", c.LogsTable)
	require.Equal(t, "secret", string(c.Password))
	require.False(t, c.QueueSettings.HasValue())
	require.False(t, c.BackOffConfig.Enabled)
}
