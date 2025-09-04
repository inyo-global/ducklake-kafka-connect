# Ducklake Kafka Sink Connector

Kafka Connect sink connector for ingesting data into DuckDB ("Ducklake"). It auto-creates and evolves tables from Arrow schemas derived from Kafka Connect records and performs upserts via `MERGE INTO` when primary key columns are defined.

## Key Features
- Optional automatic table creation using Arrow -> DuckDB type mapping
- Schema evolution (ADD COLUMN / widening upgrades for integer types and FLOAT→DOUBLE)
- Complex types (STRUCT, LIST, MAP) serialized as JSON
- Upsert semantics based on configured ID (primary key) columns (`ON CONFLICT DO UPDATE`); falls back to plain INSERT when no key
- Logical partition metadata (future use)

## Current Limitations
- JSON columns do not evolve to or from other data types (intentional safeguard).

## Build
```bash
./gradlew clean build
```
Artifact: `build/libs/ducklake-kafka-connect-<version>.jar`.

## Quick Start (Distributed Worker)
1. Copy the JAR to `plugins/ducklake/` (or another directory listed in `plugin.path`)
2. Add that directory to `plugin.path` in `connect-distributed.properties`
3. Restart the Kafka Connect worker
4. POST the connector config (see example below) to `http://<worker-host>:8083/connectors`

## Configuration (Core Properties)
See `DucklakeSinkConfig` for authoritative definitions.

| Property | Required | Description |
|----------|----------|-------------|
| `ducklake.catalog_uri` | yes | Catalog URI (e.g. `postgres:dbname=ducklake_catalog host=localhost`) |
| `ducklake.tables` | yes | Comma-separated list of destination table names |
| `topic2table.map` | no | Explicit topic→table mapping (`topicA:tbl_a,topicB:tbl_b`) |
| `ducklake.data_path` | yes | Base data path (s3://, gs://, file://) if applicable |
| `s3.url_style` | depends | `vhost` or `path` |
| `s3.use_ssl` | depends | `true` / `false` |
| `s3.endpoint` | no | Custom S3-compatible endpoint |
| `s3.access_key_id` | depends | Access key id |
| `s3._secret_access_key` | depends | Access key secret (note the underscore naming) |

### Table-Specific Properties (replace `<table>`)
- `ducklake.table.<table>.id-columns` : primary key columns (e.g. `id,tenant_id`)
- `ducklake.table.<table>.partition-by` : logical partition columns
- `ducklake.table.<table>.auto-create` : `true` / `false`

## Example Connector Config (Kafka Connect REST)
```json
{
  "name": "ducklake-sink",
  "config": {
    "connector.class": "com.inyo.ducklake.connect.DucklakeSinkConnector",
    "tasks.max": "1",
    "topics": "orders",
    "ducklake.catalog_uri": "postgres:dbname=ducklake_catalog host=localhost user=duck pwd=duck",
    "ducklake.tables": "order",
    "topic2table.map": "orders:order",
    "ducklake.data_path": "file:///tmp/ducklake/",
    "ducklake.table.order.id-columns": "id",
    "ducklake.table.order.auto-create": "true"
  }
}
```

## Schema Evolution Rules
- New columns: `ALTER TABLE ADD COLUMN`
- Allowed widening: integer ladder (TINYINT→SMALLINT→INTEGER→BIGINT) and FLOAT→DOUBLE
- Incompatible: any change involving JSON type or narrowing precision

## Internal Structure (Overview)
- `DucklakeSinkConnector` / `DucklakeSinkTask`: Kafka Connect integration
- `SinkRecordToArrowConverter`: batch conversion SinkRecord → Arrow
- `DucklakeWriter` / `DucklakeTableManager`: upsert logic & schema management

## Testing
```bash
./gradlew test
```

## Contributing
PRs and issues welcome. Style: Google Java Format + SpotBugs (build fails on violations).

## License
Apache License 2.0 (see `LICENSE.md`).
