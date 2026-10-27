// Copyright The Databend Exporter Authors
// SPDX-License-Identifier: Apache-2.0

package databendexporter

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestQueueDrainsWithoutMutatingInput(t *testing.T) {
	var records atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reader, err := r.MultipartReader()
		require.NoError(t, err)
		part, err := reader.NextPart()
		require.NoError(t, err)
		body, err := io.ReadAll(part)
		require.NoError(t, err)
		count := bytes.Count(body, []byte{'\n'})
		records.Add(int64(count))
		fmt.Fprintf(w, `{"id":"queued","stats":{"rows":%d}}`, count)
	}))
	defer server.Close()
	c := testConfig(server.URL)
	q := exporterhelper.NewDefaultQueueConfig()
	q.NumConsumers = 2
	q.QueueSize = 10
	c.QueueSettings = configoptional.Some(q)
	exp, err := NewFactory().CreateLogs(t.Context(), exportertest.NewNopSettings(componentType), c)
	require.NoError(t, err)
	require.NoError(t, exp.Start(t.Context(), componenttest.NewNopHost()))
	defer exp.Shutdown(context.Background())
	ld, err := (&plog.JSONUnmarshaler{}).UnmarshalLogs([]byte(logsFixture))
	require.NoError(t, err)
	before, err := (&plog.JSONMarshaler{}).MarshalLogs(ld)
	require.NoError(t, err)
	for range 5 {
		require.NoError(t, exp.ConsumeLogs(t.Context(), ld))
	}
	require.Eventually(t, func() bool { return records.Load() == 10 }, 5*time.Second, time.Millisecond)
	after, err := (&plog.JSONMarshaler{}).MarshalLogs(ld)
	require.NoError(t, err)
	require.JSONEq(t, string(before), string(after))
}

func TestFactoryDoesNotRetryPermanentFailure(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(http.StatusUnauthorized) }))
	defer server.Close()
	c := testConfig(server.URL)
	c.BackOffConfig.Enabled = true
	c.BackOffConfig.InitialInterval = time.Millisecond
	c.BackOffConfig.MaxInterval = time.Millisecond
	c.BackOffConfig.MaxElapsedTime = 10 * time.Millisecond
	exp, err := NewFactory().CreateLogs(t.Context(), exportertest.NewNopSettings(componentType), c)
	require.NoError(t, err)
	require.NoError(t, exp.Start(t.Context(), componenttest.NewNopHost()))
	defer exp.Shutdown(context.Background())
	ld, err := (&plog.JSONUnmarshaler{}).UnmarshalLogs([]byte(logsFixture))
	require.NoError(t, err)
	require.True(t, consumererror.IsPermanent(exp.ConsumeLogs(t.Context(), ld)))
	require.Equal(t, int32(1), calls.Load())
}
