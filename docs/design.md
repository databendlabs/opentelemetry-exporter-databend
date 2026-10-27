# Design notes and upstream path

## References consulted

- [ClickHouse exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/e82bb3592dc3757e0f0f00bf60e14099f24a01b4/exporter/clickhouseexporter):
  per-signal factory functions, exporterhelper lifecycle, queue/retry configuration
  and signal-specific schemas. The Databend implementation is not a port of
  ClickHouse's MergeTree DDL, protocol, driver or insert settings.
- [Databend streaming load handler](https://github.com/databendlabs/databend/blob/536a8997a514b14e10f2dcdf49fad44eeb2b7368/src/query/service/src/servers/http/v1/streaming_load.rs):
  PUT `/v1/streaming_load`, multipart upload, `X-Databend-SQL`, and synchronous
  acknowledgement containing `id` and `stats.rows`.
- [Databend streaming load pipeline](https://github.com/databendlabs/databend/blob/536a8997a514b14e10f2dcdf49fad44eeb2b7368/src/query/storages/stage/src/streaming_load.rs):
  `OnErrorMode::AbortNum(1)`. Streaming INSERT SQL does **not** accept the COPY
  statement's `ON_ERROR` clause; this was also verified on a real server.
- [Collector exporter helpers](https://pkg.go.dev/go.opentelemetry.io/collector/exporter/exporterhelper):
  use standard lifecycle, telemetry, queue, retry and timeout behavior rather
  than implementing an independent scheduler.
- [OTLP JSON mapping](https://opentelemetry.io/docs/specs/otlp/#json-protobuf-encoding):
  preserve typed AnyValue attributes, hex IDs and string-encoded 64-bit integers.

## Decisions

1. **Native HTTP streaming load.** It handles an entire batch in one upload,
   avoids SQL literals for user data and reuses Collector HTTP TLS/auth settings.
   A custom transport is smaller than adding an SQL driver plus its batching,
   staging and connection lifecycle. Optional automatic schema provisioning
   uses the native query API, including asynchronous pagination/finalization.
   Set `create_schema: false` to provision separately without startup DDL privileges.
2. **One row per log/span/data point.** Databend VARIANT is a natural fit for OTLP
   objects. Only common filtering fields are projected into scalar columns.
   Metric metadata is retained in each row and not flattened into lossy maps.
3. **RawMessage intermediate encoding.** Use pdata's official JSON marshalers
   instead of hand-maintaining serializers for every OTLP field. Extract record
   boundaries without converting integers through float64. This costs extra
   encoding and copies; no throughput claim is made without benchmarks.
4. **UInt64 nanoseconds.** Databend timestamps do not retain all nine fractional
   digits. Raw timestamps, point integers and IDs must round-trip exactly.
5. **At-least-once, not exactly-once.** Retry ambiguous acknowledgements rather
   than falsely report successful delivery, but explicitly document possible
   duplicate inserts. No stable deduplication ID is available from Collector's
   push API across every replay/restart.
6. **Development status.** The implementation is initially validated against a
   local Databend v1.2.831-nightly instance. Cloud IAM/token/routing behavior,
   persistent-queue crash recovery, production retention, and performance at
   scale require additional validation before claiming production readiness.

## Upstream contribution path

Current [contrib donation requirements](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/e82bb3592dc3757e0f0f00bf60e14099f24a01b4/docs/new-components.md)
require developing and hosting new components outside contrib first. Before
requesting donation:

1. Publish usable releases and gather community feedback on this standalone
   component. A custom Collector can already consume it without contrib.
2. Have a human open a component-donation issue describing the motivation,
   alternatives, supported signals and configuration.
3. Find a consenting sponsor (approver/maintainer) from another company and at
   least three codeowners, including an approver/maintainer. Do not list people
   without consent or imply that a PR author is automatically a codeowner.
4. Agree on the implementation direction with maintainers before starting the
   contrib donation PRs. Follow the requested split between initial structure,
   implementation and alpha/distribution registration.
5. Follow contrib's AI contribution policy: human-written PR descriptions and
   issue/review discussion, and no agent-checked human authorship attestation.

Human contact for this implementation: **@hantmac**. This does not assert that
human review has already happened or that upstream has accepted the component.
