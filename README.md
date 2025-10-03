# Ducklake Kafka Sink Connector

Kafka Connect sink connector for ingesting data into DuckDB ("Ducklake"). It auto-creates and evolves tables from Arrow schemas derived from Kafka Connect records and performs upserts via `MERGE INTO` when primary key columns are defined.

## Key Features
- Optional automatic table creation using Arrow -> DuckDB type mapping
- Schema evolution (ADD COLUMN / widening upgrades for integer types and FLOAT→DOUBLE)
- Complex types (STRUCT, LIST, MAP) serialized as JSON
- Upsert semantics based on configured IDs (primary keys) using merge into
- **Flexible partition expressions** supporting raw columns, temporal functions, and integer timestamp casting

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
- `ducklake.table.<table>.partition-by` : partition expressions (see Partition Expressions section below)
- `ducklake.table.<table>.auto-create` : `true` / `false`

## Partition Expressions

The connector supports flexible partition expressions that go beyond simple column names. You can use temporal functions, casting expressions, and complex SQL expressions in the `partition-by` configuration.

### Basic Column Partitioning
Partition by simple column values:
```
ducklake.table.users.partition-by=region,status
ducklake.table.orders.partition-by=customer_type,order_status
```

### Temporal Function Partitioning
Partition timestamp columns by year, month, or day:
```
ducklake.table.events.partition-by=year(created_at),month(created_at)
ducklake.table.logs.partition-by=year(timestamp),month(timestamp),day(timestamp)
```

### Integer Timestamp Partitioning
For integer timestamps (Unix epoch), use casting expressions to convert them to timestamps before applying temporal functions:

#### Basic Integer Timestamp Casting
Convert integer seconds to timestamp:
```
ducklake.table.events.partition-by=year(CAST(epoch_seconds AS TIMESTAMP))
ducklake.table.user_activity.partition-by=year(CAST(created_epoch AS TIMESTAMP)),month(CAST(created_epoch AS TIMESTAMP))
```

#### Millisecond Epoch Timestamps
Convert millisecond timestamps to seconds, then to timestamp:
```
ducklake.table.metrics.partition-by=year(CAST(event_time_millis / 1000 AS TIMESTAMP)),month(CAST(event_time_millis / 1000 AS TIMESTAMP))
ducklake.table.analytics.partition-by=year(CAST(timestamp_ms / 1000 AS TIMESTAMP))
```

#### Using DuckDB's to_timestamp Function
Alternative approach using DuckDB's built-in function:
```
ducklake.table.user_events.partition-by=year(to_timestamp(unix_time)),month(to_timestamp(unix_time))
ducklake.table.activity_logs.partition-by=year(to_timestamp(event_epoch))
```

#### Complex Conditional Casting
Handle different timestamp formats dynamically:
```
ducklake.table.mixed_events.partition-by=year(CASE WHEN timestamp_type = 'seconds' THEN CAST(event_time AS TIMESTAMP) ELSE CAST(event_time / 1000 AS TIMESTAMP) END),event_category
```

#### Epoch Timestamp with Offset
Handle different epoch bases (e.g., epoch since 1900 vs 1970):
```
ducklake.table.legacy_data.partition-by=year(CAST(epoch_time + 2208988800 AS TIMESTAMP)),region
```

### Mixed Partitioning
Combine temporal functions with regular columns:
```
ducklake.table.activity_logs.partition-by=year(CAST(event_time AS TIMESTAMP)),log_level,service_name
ducklake.table.user_events.partition-by=year(created_at),month(created_at),user_segment,event_type
```

### Partition Expression Guidelines
- **Comma-separated**: Multiple expressions separated by commas
- **SQL expressions**: Any valid DuckDB SQL expression can be used
- **Function support**: Use DuckDB functions like `year()`, `month()`, `day()`, `CAST()`, `to_timestamp()`
- **Case sensitivity**: DuckDB is case-insensitive for function names
- **Whitespace**: Extra whitespace around expressions is automatically trimmed

### Example Schema Mapping
For a Kafka message with integer timestamp:
```json
{
  "id": 123,
  "user_id": "user456",
  "event_type": "login",
  "created_epoch": 1696348800,
  "properties": {"source": "mobile"}
}
```

Use this partition configuration:
```
ducklake.table.user_events.partition-by=year(CAST(created_epoch AS TIMESTAMP)),month(CAST(created_epoch AS TIMESTAMP)),event_type
```

This will partition the table by:
- Year extracted from the integer timestamp
- Month extracted from the integer timestamp  
- Event type as a string column

## Example Connector Config (Kafka Connect REST)

### Basic Configuration
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

### Advanced Configuration with Integer Timestamp Partitioning
```json
{
  "name": "ducklake-events-sink",
  "config": {
    "connector.class": "com.inyo.ducklake.connect.DucklakeSinkConnector",
    "tasks.max": "2",
    "topics": "user_events,system_logs,metrics",
    "ducklake.catalog_uri": "postgres:dbname=ducklake_catalog host=localhost user=duck password=duck",
    "topic2table.map": "user_events:events,system_logs:logs,metrics:app_metrics",
    "ducklake.data_path": "s3://my-datalake/",
    "s3.url_style": "path",
    "s3.use_ssl": "true",
    "s3.endpoint": "s3.amazonaws.com",
    "s3.access_key_id": "${env:AWS_ACCESS_KEY_ID}",
    "s3.secret_access_key": "${env:AWS_SECRET_ACCESS_KEY}",
    
    "ducklake.table.events.id-columns": "event_id,user_id",
    "ducklake.table.events.partition-by": "year(CAST(created_epoch AS TIMESTAMP)),month(CAST(created_epoch AS TIMESTAMP)),event_type",
    "ducklake.table.events.auto-create": "true",
    
    "ducklake.table.logs.id-columns": "log_id",
    "ducklake.table.logs.partition-by": "year(to_timestamp(timestamp_unix)),log_level,service_name",
    "ducklake.table.logs.auto-create": "true",
    
    "ducklake.table.app_metrics.id-columns": "metric_id",
    "ducklake.table.app_metrics.partition-by": "year(CAST(event_time_millis / 1000 AS TIMESTAMP)),metric_type",
    "ducklake.table.app_metrics.auto-create": "true",
    
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false"
  }
}
```

### Multi-Table Configuration with Different Partition Strategies
```json
{
  "name": "ducklake-multi-sink",
  "config": {
    "connector.class": "com.inyo.ducklake.connect.DucklakeSinkConnector",
    "tasks.max": "3",
    "topics": "orders,users,analytics",
    "ducklake.catalog_uri": "postgres:dbname=ducklake_catalog host=localhost user=duck password=duck",
    "ducklake.data_path": "s3://analytics-bucket/",
    "s3.url_style": "path",
    "s3.use_ssl": "true",
    
    "ducklake.table.orders.id-columns": "order_id",
    "ducklake.table.orders.partition-by": "year(order_date),month(order_date),status",
    "ducklake.table.orders.auto-create": "true",
    
    "ducklake.table.users.id-columns": "user_id",
    "ducklake.table.users.partition-by": "region,user_type",
    "ducklake.table.users.auto-create": "true",
    
    "ducklake.table.analytics.id-columns": "event_id",
    "ducklake.table.analytics.partition-by": "year(CASE WHEN ts_format = 'ms' THEN CAST(timestamp_value / 1000 AS TIMESTAMP) ELSE CAST(timestamp_value AS TIMESTAMP) END),category",
    "ducklake.table.analytics.auto-create": "true"
  }
}
```
