/*
 * Copyright 2025 Inyo Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.inyo.ducklake.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.inyo.ducklake.ingestor.DucklakeWriter;
import com.inyo.ducklake.ingestor.DucklakeWriterConfig;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Integration test for integer timestamp partition expressions functionality. Tests the complete
 * flow from Kafka Connect configuration to table creation with integer timestamp casting in
 * partition expressions.
 */
class DucklakeIntegerTimestampPartitionIntegrationTest {

  DuckDBConnection conn;

  @BeforeEach
  void setup() throws Exception {
    conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:"); // in-memory
    try (var st = conn.createStatement()) {
      st.execute("ATTACH ':memory:' AS lake");
    }
  }

  @AfterEach
  void tearDown() throws Exception {
    conn.close();
  }

  private String uniqueTableName(String prefix) {
    return prefix + "_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
  }

  private DucklakeSinkConfig createConfig(String tableName, String partitionExpressions) {
    Map<String, Object> props = new HashMap<>();
    props.put("ducklake.catalog_uri", "test://localhost");
    props.put("ducklake.data_path", "s3://test-bucket/");
    props.put("s3.url_style", "path");
    props.put("s3.use_ssl", "true");
    props.put("s3.endpoint", "localhost:9000");
    props.put("s3.access_key_id", "test");
    props.put("s3.secret_access_key", "test");
    props.put("topic2table.map", ""); // Empty topic2table map to avoid null pointer
    props.put("ducklake.table." + tableName + ".auto-create", "true");
    if (partitionExpressions != null && !partitionExpressions.isEmpty()) {
      props.put("ducklake.table." + tableName + ".partition-by", partitionExpressions);
    }
    return new DucklakeSinkConfig(DucklakeSinkConfig.CONFIG_DEF, props);
  }

  @Test
  @DisplayName("End-to-end test: Integer timestamp partitioning with CAST via Kafka Connect config")
  void testIntegerTimestampPartitioningEndToEnd() throws Exception {
    String tableName = uniqueTableName("int_timestamp_e2e");
    DucklakeSinkConfig sinkConfig =
        createConfig(tableName, "year(CAST(epoch_seconds AS TIMESTAMP))");

    // Create writer factory and writer
    DucklakeWriterFactory factory = new DucklakeWriterFactory(sinkConfig, conn);
    DucklakeWriter writer = factory.create(tableName);

    assertNotNull(writer, "Writer should be created successfully");
    writer.close();

    // Verify partition expressions were properly parsed
    String[] partitionExprs = sinkConfig.getTablePartitionByExpressions(tableName);
    assertEquals(1, partitionExprs.length, "Should have one partition expression");
    assertEquals(
        "year(CAST(epoch_seconds AS TIMESTAMP))",
        partitionExprs[0],
        "Should parse CAST expression correctly");
  }

  @Test
  @DisplayName("End-to-end test: Multiple integer timestamp temporal functions")
  void testMultipleIntegerTimestampFunctionsEndToEnd() throws Exception {
    String tableName = uniqueTableName("multi_int_timestamp_e2e");
    DucklakeSinkConfig sinkConfig =
        createConfig(
            tableName,
            "year(CAST(created_epoch AS TIMESTAMP)),"
                + "month(CAST(created_epoch AS TIMESTAMP)),"
                + "day(CAST(created_epoch AS TIMESTAMP))");

    // Create writer factory and writer
    DucklakeWriterFactory factory = new DucklakeWriterFactory(sinkConfig, conn);
    DucklakeWriter writer = factory.create(tableName);

    assertNotNull(writer, "Writer should be created successfully");
    writer.close();

    // Verify partition expressions were properly parsed
    String[] partitionExprs = sinkConfig.getTablePartitionByExpressions(tableName);
    assertEquals(3, partitionExprs.length, "Should have three partition expressions");
    assertEquals(
        "year(CAST(created_epoch AS TIMESTAMP))",
        partitionExprs[0],
        "First expression should be year with CAST");
    assertEquals(
        "month(CAST(created_epoch AS TIMESTAMP))",
        partitionExprs[1],
        "Second expression should be month with CAST");
    assertEquals(
        "day(CAST(created_epoch AS TIMESTAMP))",
        partitionExprs[2],
        "Third expression should be day with CAST");
  }

  @Test
  @DisplayName("End-to-end test: Mixed integer timestamp and regular column partitioning")
  void testMixedIntegerTimestampPartitioningEndToEnd() throws Exception {
    String tableName = uniqueTableName("mixed_int_timestamp_e2e");
    DucklakeSinkConfig sinkConfig =
        createConfig(tableName, "year(CAST(event_time AS TIMESTAMP)),event_type,user_segment");

    // Create writer factory and writer
    DucklakeWriterFactory factory = new DucklakeWriterFactory(sinkConfig, conn);
    DucklakeWriter writer = factory.create(tableName);

    assertNotNull(writer, "Writer should be created successfully");
    writer.close();

    // Verify mixed partition expressions were properly parsed
    String[] partitionExprs = sinkConfig.getTablePartitionByExpressions(tableName);
    assertEquals(3, partitionExprs.length, "Should have three partition expressions");
    assertEquals(
        "year(CAST(event_time AS TIMESTAMP))",
        partitionExprs[0],
        "First expression should be year with CAST");
    assertEquals("event_type", partitionExprs[1], "Second expression should be regular column");
    assertEquals("user_segment", partitionExprs[2], "Third expression should be regular column");
  }

  @Test
  @DisplayName("End-to-end test: Millisecond epoch timestamp conversion")
  void testMillisecondEpochConversionEndToEnd() throws Exception {
    String tableName = uniqueTableName("millis_epoch_e2e");
    DucklakeSinkConfig sinkConfig =
        createConfig(
            tableName,
            "year(CAST(event_time_millis / 1000 AS TIMESTAMP)),"
                + "month(CAST(event_time_millis / 1000 AS TIMESTAMP))");

    // Create writer factory and writer
    DucklakeWriterFactory factory = new DucklakeWriterFactory(sinkConfig, conn);
    DucklakeWriter writer = factory.create(tableName);

    assertNotNull(writer, "Writer should be created successfully");
    writer.close();

    // Verify millisecond conversion expressions were properly parsed
    String[] partitionExprs = sinkConfig.getTablePartitionByExpressions(tableName);
    assertEquals(2, partitionExprs.length, "Should have two partition expressions");
    assertEquals(
        "year(CAST(event_time_millis / 1000 AS TIMESTAMP))",
        partitionExprs[0],
        "First expression should include millisecond conversion");
    assertEquals(
        "month(CAST(event_time_millis / 1000 AS TIMESTAMP))",
        partitionExprs[1],
        "Second expression should include millisecond conversion");
  }

  @Test
  @DisplayName("End-to-end test: to_timestamp function expressions")
  void testToTimestampFunctionEndToEnd() throws Exception {
    String tableName = uniqueTableName("to_timestamp_e2e");
    DucklakeSinkConfig sinkConfig =
        createConfig(tableName, "year(to_timestamp(unix_time)),month(to_timestamp(unix_time))");

    // Create writer factory and writer
    DucklakeWriterFactory factory = new DucklakeWriterFactory(sinkConfig, conn);
    DucklakeWriter writer = factory.create(tableName);

    assertNotNull(writer, "Writer should be created successfully");
    writer.close();

    // Verify to_timestamp expressions were properly parsed
    String[] partitionExprs = sinkConfig.getTablePartitionByExpressions(tableName);
    assertEquals(2, partitionExprs.length, "Should have two partition expressions");
    assertEquals(
        "year(to_timestamp(unix_time))",
        partitionExprs[0],
        "First expression should use to_timestamp function");
    assertEquals(
        "month(to_timestamp(unix_time))",
        partitionExprs[1],
        "Second expression should use to_timestamp function");
  }

  @Test
  @DisplayName("End-to-end test: Complex conditional CASE expressions")
  void testComplexConditionalExpressionsEndToEnd() throws Exception {
    String tableName = uniqueTableName("complex_conditional_e2e");
    DucklakeSinkConfig sinkConfig =
        createConfig(
            tableName,
            "year(CASE WHEN timestamp_type = 'seconds' THEN CAST(event_time AS TIMESTAMP) "
                + "ELSE CAST(event_time / 1000 AS TIMESTAMP) END),status");

    // Create writer factory and writer
    DucklakeWriterFactory factory = new DucklakeWriterFactory(sinkConfig, conn);
    DucklakeWriter writer = factory.create(tableName);

    assertNotNull(writer, "Writer should be created successfully");
    writer.close();

    // Verify complex conditional expressions were properly parsed
    String[] partitionExprs = sinkConfig.getTablePartitionByExpressions(tableName);
    assertEquals(2, partitionExprs.length, "Should have two partition expressions");
    assertEquals(
        "year(CASE WHEN timestamp_type = 'seconds' THEN CAST(event_time AS TIMESTAMP)"
            + " ELSE CAST(event_time / 1000 AS TIMESTAMP) END)",
        partitionExprs[0],
        "First expression should be complex CASE statement");
    assertEquals("status", partitionExprs[1], "Second expression should be regular column");
  }

  @Test
  @DisplayName("End-to-end test: Epoch timestamp with offset calculations")
  void testEpochTimestampWithOffsetEndToEnd() throws Exception {
    String tableName = uniqueTableName("epoch_offset_e2e");
    DucklakeSinkConfig sinkConfig =
        createConfig(tableName, "year(CAST(epoch_time + 2208988800 AS TIMESTAMP)),region");

    // Create writer factory and writer
    DucklakeWriterFactory factory = new DucklakeWriterFactory(sinkConfig, conn);
    DucklakeWriter writer = factory.create(tableName);

    assertNotNull(writer, "Writer should be created successfully");
    writer.close();

    // Verify offset calculation expressions were properly parsed
    String[] partitionExprs = sinkConfig.getTablePartitionByExpressions(tableName);
    assertEquals(2, partitionExprs.length, "Should have two partition expressions");
    assertEquals(
        "year(CAST(epoch_time + 2208988800 AS TIMESTAMP))",
        partitionExprs[0],
        "First expression should include offset calculation");
    assertEquals("region", partitionExprs[1], "Second expression should be regular column");
  }

  @Test
  @DisplayName("End-to-end test: DucklakeWriterConfig correctly uses integer timestamp expressions")
  void testWriterConfigUsesIntegerTimestampExpressions() throws Exception {
    String tableName = uniqueTableName("writer_config_int_timestamp_e2e");
    DucklakeSinkConfig sinkConfig =
        createConfig(tableName, "year(CAST(created_epoch AS TIMESTAMP)),status");

    // Get partition expressions from sink config
    var partitionExprs = sinkConfig.getTablePartitionByExpressions(tableName);

    // Create writer config directly
    var writerConfig =
        new DucklakeWriterConfig(tableName, true, new String[] {"id"}, partitionExprs);

    // Verify writer config has the correct partition expressions
    var configExprs = writerConfig.partitionByExpressions();
    assertEquals(2, configExprs.length, "Writer config should have two partition expressions");
    assertEquals(
        "year(CAST(created_epoch AS TIMESTAMP))",
        configExprs[0],
        "First expression should be integer timestamp with CAST");
    assertEquals("status", configExprs[1], "Second expression should be status column");
  }
}
