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

## Note about Java 11+ / --add-opens
When running the connector on Java 11 or newer (for example in Docker containers or when using Testcontainers), Apache Arrow needs access to certain internal JDK packages. You must expose the java.nio package to Arrow by adding the following JVM option via the KAFKA_OPTS environment variable for the Kafka Connect worker.

Example (used in tests with Testcontainers):
this.withEnv("KAFKA_OPTS", "--add-opens java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED");

Example (Docker / Docker Compose):
-e KAFKA_OPTS="--add-opens java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED"

Without this option the connector may fail to load internal classes used by Arrow and throw IllegalAccessError or fail to initialize.

## Note about JSON payloads / converters
If your Kafka messages are raw JSON (string bytes), configure the connector (or worker) to use the JsonConverter without schemas. Otherwise Kafka Connect will attempt to deserialize bytes with the default converter and you may see errors like "Converting byte[] to Kafka Connect data failed due to serialization error".

Recommended connector config (add to the connector "config" payload):
```
"key.converter": "org.apache.kafka.connect.json.JsonConverter",
"key.converter.schemas.enable": "false",
"value.converter": "org.apache.kafka.connect.json.JsonConverter",
"value.converter.schemas.enable": "false"
```
Alternatively, set the worker environment variables (Docker/Compose/Testcontainers):

CONNECT_KEY_CONVERTER=org.apache.kafka.connect.json.JsonConverter
CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE=false
CONNECT_VALUE_CONVERTER=org.apache.kafka.connect.json.JsonConverter
CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE=false

## Configuration (Core Properties)
See `DucklakeSinkConfig` for authoritative definitions.

| Property                | Required | Description                                                          |
|-------------------------|----------|----------------------------------------------------------------------|
| `ducklake.catalog_uri`  | yes      | Catalog URI (e.g. `postgres:dbname=ducklake_catalog host=localhost`) |
| `topic2table.map`       | no       | Explicit topic→table mapping (`topicA:tbl_a,topicB:tbl_b`)           |
| `ducklake.data_path`    | yes      | Base data path (s3://, gs://, file://) if applicable                 |
| `s3.url_style`          | depends  | `vhost` or `path`                                                    |
| `s3.use_ssl`            | depends  | `true` / `false`                                                     |
| `s3.endpoint`           | no       | Custom S3-compatible endpoint                                        |
| `s3.access_key_id`      | depends  | Access key id                                                        |
| `s3.secret_access_key` | depends  | Access key secret (note the underscore naming)                       |

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
    "ducklake.catalog_uri": "postgres:dbname=ducklake_catalog host=localhost user=duck password=duck",
    "topic2table.map": "orders:orders",
    "ducklake.data_path": "file:///tmp/ducklake/",
    "ducklake.table.orders.id-columns": "id",
    "ducklake.table.orders.auto-create": "true",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false"
  }
}
```

## Note about using MinIO in integration tests
The integration tests use a local MinIO container as an S3-compatible backend so that Kafka Connect (running in a container), the connector, and the host can all access the same data path.

If you run the connector against MinIO in tests, use an S3 path and set the S3 properties accordingly. Example (matches the integration test):

- ducklake.data_path: s3://test-bucket/
- s3.url_style: path
- s3.use_ssl: false
- s3.endpoint: http://minio:9000
- s3.access_key_id: minio
- s3.secret_access_key: minio123

Note: The test creates the bucket `test-bucket` in MinIO before starting the connector (using the minio client). In local or CI setups, ensure the bucket exists or create it programmatically before the connector starts.

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

