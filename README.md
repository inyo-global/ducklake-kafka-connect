# Ducklake Kafka Sink Connector

Kafka Connect sink connector for ingesting data into DuckDB ("Ducklake"). It auto-creates and evolves tables from Arrow schemas derived from Kafka Connect records and performs upserts via `MERGE INTO` when primary key columns are defined.

## Key Features
- Optional automatic table creation using Arrow -> DuckDB type mapping
- Schema evolution (ADD COLUMN / widening upgrades for integer types and FLOAT→DOUBLE)
- Complex types (STRUCT, LIST, MAP) serialized as JSON
- Upsert semantics based on configured IDs (primary keys) using merge into
- partition supporting raw columns and temporal functions

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

## Note about --add-opens
When running the connector:

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

## Arrow IPC Converter Support

The Ducklake connector now includes native support for **Arrow IPC (Inter-Process Communication)** data format through the `ArrowIpcConverter`. This allows you to send pre-serialized Arrow data directly to DuckDB without the overhead of JSON conversion.

### Benefits of Arrow IPC
- **High Performance**: Direct binary format without JSON parsing overhead
- **Type Safety**: Preserves exact Arrow types and schemas
- **Memory Efficiency**: Columnar format optimized for analytics
- **Zero-Copy**: Direct processing of Arrow data structures

### Using Arrow IPC Converter

Configure your connector to use the Arrow IPC converter for values:

```json
{
  "name": "ducklake-arrow-sink",
  "config": {
    "connector.class": "com.inyo.ducklake.connect.DucklakeSinkConnector",
    "topics": "arrow-data-topic",
    "value.converter": "com.inyo.ducklake.connect.ArrowIpcConverter",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "ducklake.connection.url": "jdbc:duckdb:/path/to/database.db",
    "ducklake.table.auto.create": "true"
  }
}
```

### Python Example with PyArrow

You can also produce Arrow IPC data from Python:

```python
import pyarrow as pa
import pyarrow.ipc as ipc
from kafka import KafkaProducer
import io

# Create Arrow table
data = {
    'id': [1001, 1002, 1003],
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35],
    'timestamp': [1640995200000, 1640995260000, 1640995320000]
}

table = pa.table(data)

# Serialize to Arrow IPC bytes
buffer = io.BytesIO()
with ipc.new_stream(buffer, table.schema) as writer:
    writer.write_table(table)

arrow_ipc_bytes = buffer.getvalue()

# Send to Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: x  # Send raw bytes
)

producer.send('arrow-data-topic', value=arrow_ipc_bytes)
producer.flush()
```

### Mixed Data Support

The connector automatically detects the data format and handles both traditional JSON data and Arrow IPC data seamlessly:

- **Traditional data**: Processed using `SinkRecordToArrowConverter` (JSON → Arrow → DuckDB)
- **Arrow IPC data**: Processed directly as `VectorSchemaRoot` (Arrow IPC → DuckDB)

This means you can migrate gradually from JSON to Arrow IPC without changing your connector configuration.

### Performance Considerations

When using Arrow IPC:
- **Batch size**: Larger batches generally perform better due to columnar processing
- **Schema consistency**: Keep schemas consistent across batches for optimal performance
- **Memory allocation**: Arrow uses off-heap memory; monitor memory usage in high-throughput scenarios

### Troubleshooting Arrow IPC

Common issues and solutions:

1. **OutOfMemoryError**: Increase JVM heap size or reduce batch sizes
2. **Schema mismatch**: Ensure consistent Arrow schemas across producers
3. **Invalid IPC data**: Verify Arrow IPC serialization is correct

Example JVM settings for Arrow workloads:
```
-Xmx4g -XX:MaxDirectMemorySize=2g
```

## Configuration (Core Properties)
See `DucklakeSinkConfig` for authoritative definitions.

| Property               | Required | Description                                                          |
|------------------------|----------|----------------------------------------------------------------------|
| `ducklake.catalog_uri` | yes      | Catalog URI (e.g. `postgres:dbname=ducklake_catalog host=localhost`) |
| `topic2table.map`      | no       | Explicit topic→table mapping (`topicA:tbl_a,topicB:tbl_b`)           |
| `ducklake.data_path`   | yes      | Base data path (s3://, gs://, file://) if applicable                 |
| `s3.url_style`         | depends  | `vhost` or `path`                                                    |
| `s3.use_ssl`           | depends  | `true` / `false`                                                     |
| `s3.endpoint`          | no       | Custom S3-compatible endpoint                                        |
| `s3.access_key_id`     | depends  | Access key id                                                        |
| `s3.secret_access_key` | depends  | Access key secret (note the underscore naming)                       |

### Table-Specific Properties (replace `<table>`)
- `ducklake.table.<table>.id-columns` : primary key columns (e.g. `id,tenant_id`)
- `ducklake.table.<table>.partition-by` : partition expressions (see Partition Expressions section below)
- `ducklake.table.<table>.auto-create` : `true` / `false`

## Partition Expressions

The connector supports flexible partition expressions that go beyond simple column names. You can use temporal functions in the `partition-by` configuration.

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

### Mixed Partitioning
Combine temporal functions with regular columns:
```
ducklake.table.user_events.partition-by=year(created_at),month(created_at),user_segment,event_type
```

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
"transforms" = "TimestampConverter"
"transforms.TimestampConverter.type"        = "org.apache.kafka.connect.transforms.TimestampConverter$Value",
"transforms.TimestampConverter.field"       = "created_epoch"
"transforms.TimestampConverter.format"      = "yyyy-MM-dd'T'HH:mm:ss"
"transforms.TimestampConverter.target.type" = "string"     
ducklake.table.user_events.partition-by=year(created_epoch),month(created_epoch),event_type
```

This will partition the table by:
- Year extracted from the integer timestamp
- Month extracted from the integer timestamp  
- Event type as a string column

## Example Connector Config (Kafka Connect REST)

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
    "ducklake.table.events.partition-by": "year(created_epoch),month(created_epoch),event_type",
    "ducklake.table.events.auto-create": "true",
    
    "ducklake.table.logs.id-columns": "log_id",
    "ducklake.table.logs.auto-create": "true",
    
    "ducklake.table.app_metrics.id-columns": "metric_id",
    "ducklake.table.app_metrics.auto-create": "true",
    
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false"
  }
}
```
