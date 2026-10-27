# Databend exporter for the OpenTelemetry Collector

An **in-development**, standalone Collector component that exports **logs, traces,
and metrics** to [Databend](https://github.com/databendlabs/databend). This is not
part of the official OpenTelemetry Collector Contrib distribution.

The exporter uses Databend's native HTTP streaming load endpoint. It does not use
the ClickHouse wire protocol, depend on a ClickHouse compatibility layer, or
require a ClickHouse server. It can create the target database and signal tables
on startup, or use an operator-provisioned schema.

## Quick start

Requires Go 1.26+ and a Databend HTTP endpoint with `/v1/streaming_load` enabled.

1. Choose schema provisioning. By default, `create_schema: true` automatically
   creates the configured database and enabled signal tables. The startup account
   needs CREATE privileges. For least-privilege deployments, provision the tables
   using [`schema.sql`](schema.sql), then set `create_schema: false`:

   ```sh
   bendsql --dsn "$DATABEND_DSN" < schema.sql
   ```

   Use a provisioning account for DDL and a separate account with INSERT
   privileges on the three tables for the exporter with `create_schema: false`.
   `database` must match the database used in the SQL file. Schema migrations are the operator's
   responsibility; `CREATE TABLE IF NOT EXISTS` does not migrate existing tables.

2. Build a custom Collector using [Collector Builder](https://opentelemetry.io/docs/collector/custom-collector/):

   ```sh
   go run go.opentelemetry.io/collector/cmd/builder@v0.164.0 \
     --config examples/builder-config.yaml
   ```

   Run this command from the repository root. The local module replacement in
   the builder config allows building before the component has a release tag.
   Match Collector dependency versions when adding this module to another build.

3. Start the Collector:

   ```sh
   export DATABEND_ENDPOINT=http://localhost:8000
   export DATABEND_USERNAME=databend
   export DATABEND_PASSWORD=databend
   ./_build/otelcol-databend --config examples/collector.yaml
   ```

   Send OTLP to `127.0.0.1:4317` (gRPC) or `127.0.0.1:4318` (HTTP). The example
   deliberately binds receivers to loopback. For a network deployment, configure
   receiver TLS/authentication before exposing these ports.

For Databend Cloud, use the warehouse's **HTTPS origin** and its credentials.
TLS verification is on by default. Supply `tls.ca_file` for a private CA; do not
disable certificate verification in production. Cloud authentication/routing
varies by deployment and is not covered by the local integration test.

## Configuration

```yaml
exporters:
  databend:
    endpoint: https://warehouse.example.com
    username: ${env:DATABEND_USERNAME}
    password: ${env:DATABEND_PASSWORD}
    database: default
    create_schema: true
    logs_table_name: otel_logs
    traces_table_name: otel_traces
    metrics_table_name: otel_metrics
    timeout: 30s
    sending_queue:
      enabled: true
      num_consumers: 4
      queue_size: 1000
    retry_on_failure:
      enabled: true
      initial_interval: 5s
      max_interval: 30s
      max_elapsed_time: 300s
```

| Setting | Default | Meaning |
| --- | --- | --- |
| `endpoint` | required | Databend HTTP(S) origin, without credentials, paths or query parameters |
| `username` | required unless `auth` is set | HTTP Basic authentication username |
| `password` | empty | Opaque password; prefer environment-variable expansion |
| `auth` | unset | Standard Collector HTTP auth extension, instead of Basic authentication |
| `create_schema` | `true` | Create the database and enabled signal tables at startup; does not migrate existing tables |
| `database` | `default` | Target database |
| `logs_table_name` | `otel_logs` | Logs table |
| `traces_table_name` | `otel_traces` | Spans table |
| `metrics_table_name` | `otel_metrics` | Metric data points table |
| `timeout` | `30s` | Positive per-attempt timeout, including response-body consumption |
| `sending_queue` | standard Collector defaults, enabled | Queue settings; supports Collector persistent storage extensions |
| `retry_on_failure` | standard Collector defaults, enabled | Retry backoff and maximum elapsed time |
| `tls`, `headers`, `auth`, HTTP transport settings | standard Collector defaults | See [confighttp](https://pkg.go.dev/go.opentelemetry.io/collector/config/confighttp) |

Identifiers must match `[A-Za-z_][A-Za-z0-9_]*`, and the three table names must be
distinct. SQL, content-type and authentication headers are reserved. Use `auth`
for bearer-token authentication rather than an `Authorization` header. HTTP
redirects are rejected to avoid forwarding telemetry and credentials elsewhere.
Errors deliberately omit server response text, which can contain telemetry or
credentials.

## Storage model

Each row stores one log record, one span, or **one metric data point**. All tables
use the same initial schema to keep provisioning simple. `metric_type` is empty
for logs/traces, and `trace_id` / `span_id` are empty for metrics (exemplar IDs
remain in `record`). This is not a schema compatible with `clickhouseexporter`.

| Column | Type | Meaning |
| --- | --- | --- |
| `time_unix_nano` | `UInt64` | Log timestamp (observed timestamp if unset), span start, or data point timestamp |
| `service_name` | `String` | Resource's string-valued `service.name`, or empty |
| `trace_id`, `span_id` | `String` | Hex IDs for logs/spans, or empty for unset IDs |
| `name` | `String` | Log event name, span name, or metric name |
| `metric_type` | `String` | OTLP type: `gauge`, `sum`, `histogram`, `exponentialHistogram`, `summary` |
| `resource` | `VARIANT` | Full OTLP Resource JSON, including typed attributes and dropped counts |
| `resource_schema_url` | `String` | Resource schema URL |
| `scope` | `VARIANT` | Full InstrumentationScope JSON, including attributes and dropped counts |
| `scope_schema_url` | `String` | Scope schema URL |
| `record` | `VARIANT` | Full OTLP log/span JSON, or metric JSON containing exactly one data point |

Metric metadata (name, description, unit, temporality, monotonicity) is repeated
per point. All five metric types are supported, including exemplars, explicit
and exponential buckets, min/max, quantiles and flags. No aggregation or
conversion between cumulative and delta temporality is performed. Metrics with
no data points produce no rows.

Structured log bodies, bytes, nested attribute types, span events/links, status,
trace state, flags and dropped counts remain in their original OTLP JSON shape.
64-bit OTLP integer strings are not decoded through `float64`. Raw nanosecond
index columns avoid Databend TIMESTAMP's microsecond precision limit. OTLP
special doubles (`NaN`, `Infinity`, `-Infinity`) remain JSON strings. This design
favors faithful ingestion over a separate optimized schema for every metric
type; tune clustering, derived columns and retention for your workload.

Example queries:

```sql
SELECT time_unix_nano, service_name, record:body
FROM default.otel_logs
WHERE service_name = 'checkout'
ORDER BY time_unix_nano DESC LIMIT 100;

SELECT span_id, name, record:events, record:status
FROM default.otel_traces
WHERE trace_id = '0102030405060708090a0b0c0d0e0f10';

SELECT time_unix_nano, record:sum:dataPoints[0]:asInt::BIGINT AS value
FROM default.otel_metrics
WHERE name = 'requests' AND metric_type = 'sum';
```

## Delivery and operational limits

- Inserts use multipart NDJSON streaming load, whose server-side default aborts
  on the first malformed row instead of silently skipping it. Success requires an
  acknowledgement with a query ID and the expected row count.
- Network failures, HTTP 408/429/5xx and ambiguous acknowledgements are retryable.
  Other HTTP errors, including redirects and 401/403, are permanent. Databend
  versions may report SQL/runtime errors as HTTP 400; those are treated as
  permanent and require operator attention.
- Delivery is **at least once during successful retries**, not exactly once.
  If a connection fails after commit, retrying may duplicate rows. There is no
  insert deduplication. Queue overflow, exhausted retry budgets, permanent
  errors, or process termination without persistent storage can lose data.
- Queueing, concurrency and retries use Collector exporter helpers. Enable a
  persistent queue for crash resilience. Use the batch and memory-limiter
  processors; the example caps batches at 1024 items. Serialization and multipart
  buffers are held in memory, and record size is not bounded by item count.
- Each signal has its own client/queue and writes independently; there is no
  cross-signal transaction, ordering guarantee or automatic TTL.
- Profiles and automatic schema migration are not implemented. With
  `create_schema: true`, startup creates the database and each enabled signal's
  table using idempotent DDL. Existing schema compatibility is the operator's
  responsibility. Disable schema creation for an INSERT-only account.

## Development and testing

```sh
go test -race -cover ./...
go vet ./...
```

An opt-in integration test provisions a unique temporary database, writes all
three signals through the actual factory/exporter helpers, queries the stored
values, checks all five metric types and nanosecond/integer precision, and drops
that database:

```sh
docker run --rm -d --name otel-databend-test \
  -p 127.0.0.1:18900:8000 \
  -e QUERY_DEFAULT_USER=databend -e QUERY_DEFAULT_PASSWORD=databend \
  datafuselabs/databend:v1.2.831-nightly
# Wait until the HTTP endpoint responds, then:
DATABEND_ENDPOINT=http://127.0.0.1:18900 \
DATABEND_USERNAME=databend DATABEND_PASSWORD=databend \
  go test -run TestIntegration -v -count=1 .
docker stop otel-databend-test
```

The test account needs CREATE/DROP DATABASE privileges; a production exporter
account does not when `create_schema: false`. CI runs unit/race tests, static
analysis, integration/OTLP end-to-end tests and a custom Collector build. See [design notes](docs/design.md) for the source
references and upstream contribution path.

## License

Apache-2.0. See [LICENSE](LICENSE).
