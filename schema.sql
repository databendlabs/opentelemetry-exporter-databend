-- For manual provisioning (create_schema: false), run as an administrator.
-- The exporter embeds the first CREATE statement for automatic provisioning.
-- Change `default` consistently here and in the exporter configuration.
-- UInt64 timestamps retain OTLP's full nanosecond precision.
CREATE TABLE IF NOT EXISTS default.otel_logs (
    time_unix_nano UInt64,
    service_name String,
    trace_id String,
    span_id String,
    name String,
    metric_type String,
    resource VARIANT,
    resource_schema_url String,
    scope VARIANT,
    scope_schema_url String,
    record VARIANT
);

CREATE TABLE IF NOT EXISTS default.otel_traces LIKE default.otel_logs;
CREATE TABLE IF NOT EXISTS default.otel_metrics LIKE default.otel_logs;
