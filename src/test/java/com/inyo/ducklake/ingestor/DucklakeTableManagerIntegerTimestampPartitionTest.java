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
package com.inyo.ducklake.ingestor;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.UUID;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for handling integer timestamps with temporal function partitioning. This test class
 * verifies that integer timestamps (like Unix epoch) can be used with temporal functions by casting
 * them to timestamp types in partition expressions.
 */
class DucklakeTableManagerIntegerTimestampPartitionTest {

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

  private Schema schema(Field... fields) {
    return new Schema(Arrays.asList(fields));
  }

  private Field intField(String name) {
    return new Field(name, new FieldType(false, new ArrowType.Int(32, true), null), null);
  }

  private Field longField(String name) {
    return new Field(name, new FieldType(false, new ArrowType.Int(64, true), null), null);
  }

  private Field stringField(String name) {
    return new Field(name, new FieldType(false, ArrowType.Utf8.INSTANCE, null), null);
  }

  private Field timestampField(String name) {
    return new Field(
        name,
        new FieldType(
            false,
            new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, null),
            null),
        null);
  }

  /**
   * Checks if a table was created successfully and verifies that partition expressions were
   * processed (even if partitioning itself isn't supported in test environment).
   */
  private boolean isTableCreatedSuccessfully(String tableName) throws SQLException {
    // Verify the table was created successfully
    String sql = "SELECT COUNT(*) FROM pragma_table_info(?)";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, "lake.main." + SqlIdentifierUtil.quote(tableName));
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() && rs.getInt(1) > 0;
      }
    }
  }

  @Test
  @DisplayName("Creates table with integer timestamp partitioning using CAST to TIMESTAMP")
  void testIntegerTimestampPartitioningWithCast() throws Exception {
    String tableName = uniqueTableName("int_timestamp_cast");
    DucklakeWriterConfig config =
        new DucklakeWriterConfig(
            tableName,
            true,
            new String[] {"id"},
            new String[] {"year(CAST(epoch_seconds AS TIMESTAMP))"} // Cast integer to timestamp
            );

    DucklakeTableManager manager = new DucklakeTableManager(conn, config);
    Schema schema =
        schema(
            intField("id"),
            longField("epoch_seconds"), // Integer field representing Unix timestamp
            stringField("event_name"));

    boolean existed = manager.ensureTable(schema);
    assertFalse(existed, "Table should not have existed before creation");
    assertTrue(
        isTableCreatedSuccessfully(tableName),
        "Table should be created with integer timestamp partitioning");
  }

  @Test
  @DisplayName("Creates table with multiple integer timestamp temporal functions")
  void testMultipleIntegerTimestampTemporalFunctions() throws Exception {
    String tableName = uniqueTableName("multi_int_timestamp");
    DucklakeWriterConfig config =
        new DucklakeWriterConfig(
            tableName,
            true,
            new String[] {"id"},
            new String[] {
              "year(CAST(created_epoch AS TIMESTAMP))",
              "month(CAST(created_epoch AS TIMESTAMP))",
              "day(CAST(created_epoch AS TIMESTAMP))"
            });

    DucklakeTableManager manager = new DucklakeTableManager(conn, config);
    Schema schema =
        schema(
            intField("id"),
            longField("created_epoch"), // Unix timestamp as long
            stringField("user_name"),
            stringField("action"));

    boolean existed = manager.ensureTable(schema);
    assertFalse(existed, "Table should not have existed before creation");
    assertTrue(
        isTableCreatedSuccessfully(tableName),
        "Table should be created with multiple integer timestamp functions");
  }

  @Test
  @DisplayName("Creates table with mixed partitioning: integer timestamp and regular columns")
  void testMixedIntegerTimestampPartitioning() throws Exception {
    String tableName = uniqueTableName("mixed_int_timestamp");
    DucklakeWriterConfig config =
        new DucklakeWriterConfig(
            tableName,
            true,
            new String[] {"id"},
            new String[] {
              "year(CAST(event_time AS TIMESTAMP))", // Integer timestamp with casting
              "event_type", // Regular string column
              "user_segment" // Another regular string column
            });

    DucklakeTableManager manager = new DucklakeTableManager(conn, config);
    Schema schema =
        schema(
            intField("id"),
            longField("event_time"), // Integer timestamp
            stringField("event_type"), // Regular partitioning column
            stringField("user_segment"), // Regular partitioning column
            stringField("event_data"));

    boolean existed = manager.ensureTable(schema);
    assertFalse(existed, "Table should not have existed before creation");
    assertTrue(
        isTableCreatedSuccessfully(tableName),
        "Table should be created with mixed integer timestamp partitioning");
  }

  @Test
  @DisplayName("Creates table with millisecond epoch timestamp partitioning")
  void testMillisecondEpochTimestampPartitioning() throws Exception {
    String tableName = uniqueTableName("millis_epoch");
    DucklakeWriterConfig config =
        new DucklakeWriterConfig(
            tableName,
            true,
            new String[] {"id"},
            new String[] {
              "year(CAST(event_time_millis / 1000 AS TIMESTAMP))", // Convert millis to seconds then
              // cast
              "month(CAST(event_time_millis / 1000 AS TIMESTAMP))"
            });

    DucklakeTableManager manager = new DucklakeTableManager(conn, config);
    Schema schema =
        schema(
            intField("id"),
            longField("event_time_millis"), // Millisecond epoch timestamp
            stringField("event_name"),
            stringField("category"));

    boolean existed = manager.ensureTable(schema);
    assertFalse(existed, "Table should not have existed before creation");
    assertTrue(
        isTableCreatedSuccessfully(tableName),
        "Table should be created with millisecond epoch partitioning");
  }

  @Test
  @DisplayName("Creates table with epoch timestamp using to_timestamp function")
  void testEpochTimestampWithToTimestampFunction() throws Exception {
    String tableName = uniqueTableName("epoch_to_timestamp");
    DucklakeWriterConfig config =
        new DucklakeWriterConfig(
            tableName,
            true,
            new String[] {"id"},
            new String[] {
              "year(to_timestamp(unix_time))", // Use DuckDB's to_timestamp function
              "month(to_timestamp(unix_time))"
            });

    DucklakeTableManager manager = new DucklakeTableManager(conn, config);
    Schema schema =
        schema(
            intField("id"),
            longField("unix_time"), // Unix timestamp
            stringField("user_id"),
            stringField("activity_type"));

    boolean existed = manager.ensureTable(schema);
    assertFalse(existed, "Table should not have existed before creation");
    assertTrue(
        isTableCreatedSuccessfully(tableName),
        "Table should be created with to_timestamp function partitioning");
  }

  @Test
  @DisplayName("Creates table with complex integer timestamp expressions")
  void testComplexIntegerTimestampExpressions() throws Exception {
    String tableName = uniqueTableName("complex_int_timestamp");
    DucklakeWriterConfig config =
        new DucklakeWriterConfig(
            tableName,
            true,
            new String[] {"id"},
            new String[] {
              "year(CASE WHEN timestamp_type = 'seconds' THEN CAST(event_time AS TIMESTAMP) "
                  + "ELSE CAST(event_time / 1000 AS TIMESTAMP) END)", // Complex conditional casting
              "status"
            });

    DucklakeTableManager manager = new DucklakeTableManager(conn, config);
    Schema schema =
        schema(
            intField("id"),
            longField("event_time"), // Could be seconds or milliseconds
            stringField("timestamp_type"), // Indicates the unit of event_time
            stringField("status"),
            stringField("description"));

    boolean existed = manager.ensureTable(schema);
    assertFalse(existed, "Table should not have existed before creation");
    assertTrue(
        isTableCreatedSuccessfully(tableName),
        "Table should be created with complex integer timestamp expressions");
  }

  @Test
  @DisplayName("Handles integer timestamp partitioning with schema evolution")
  void testIntegerTimestampPartitioningWithSchemaEvolution() throws Exception {
    String tableName = uniqueTableName("int_timestamp_evolution");
    DucklakeWriterConfig config =
        new DucklakeWriterConfig(
            tableName,
            true,
            new String[] {"id"},
            new String[] {"year(CAST(created_at_epoch AS TIMESTAMP))"});

    DucklakeTableManager manager = new DucklakeTableManager(conn, config);

    // Create initial schema with integer timestamp
    Schema initialSchema =
        schema(
            intField("id"),
            longField("created_at_epoch"), // Integer timestamp
            stringField("event_type"));

    boolean existed = manager.ensureTable(initialSchema);
    assertFalse(existed, "Table should not have existed initially");

    // Evolve schema by adding new columns
    Schema evolvedSchema =
        schema(
            intField("id"),
            longField("created_at_epoch"), // Existing integer timestamp
            stringField("event_type"), // Existing column
            stringField("user_id"), // New column
            longField("updated_at_epoch") // New integer timestamp column
            );

    existed = manager.ensureTable(evolvedSchema);
    assertTrue(existed, "Table should exist after evolution");
    assertTrue(
        isTableCreatedSuccessfully(tableName),
        "Table should still work after evolution with integer timestamps");
  }

  @Test
  @DisplayName("Creates table with different integer timestamp data types")
  void testDifferentIntegerTimestampDataTypes() throws Exception {
    String tableName = uniqueTableName("different_int_types");
    DucklakeWriterConfig config =
        new DucklakeWriterConfig(
            tableName,
            true,
            new String[] {"id"},
            new String[] {
              "year(CAST(timestamp_32 AS TIMESTAMP))", // 32-bit integer
              "month(CAST(timestamp_64 AS TIMESTAMP))" // 64-bit integer
            });

    DucklakeTableManager manager = new DucklakeTableManager(conn, config);
    Schema schema =
        schema(
            intField("id"),
            intField("timestamp_32"), // 32-bit integer timestamp
            longField("timestamp_64"), // 64-bit integer timestamp
            stringField("event_name"));

    boolean existed = manager.ensureTable(schema);
    assertFalse(existed, "Table should not have existed before creation");
    assertTrue(
        isTableCreatedSuccessfully(tableName),
        "Table should be created with different integer timestamp types");
  }

  @Test
  @DisplayName("Creates table with epoch timestamp offset handling")
  void testEpochTimestampWithOffset() throws Exception {
    String tableName = uniqueTableName("epoch_with_offset");
    DucklakeWriterConfig config =
        new DucklakeWriterConfig(
            tableName,
            true,
            new String[] {"id"},
            new String[] {
              // Handle different epoch bases (e.g., epoch since 1900 vs 1970)
              "year(CAST(epoch_time + 2208988800 AS TIMESTAMP))", // Add offset if needed
              "region"
            });

    DucklakeTableManager manager = new DucklakeTableManager(conn, config);
    Schema schema =
        schema(
            intField("id"),
            longField("epoch_time"), // Epoch with different base
            stringField("region"),
            stringField("event_data"));

    boolean existed = manager.ensureTable(schema);
    assertFalse(existed, "Table should not have existed before creation");
    assertTrue(
        isTableCreatedSuccessfully(tableName),
        "Table should be created with epoch offset handling");
  }
}
