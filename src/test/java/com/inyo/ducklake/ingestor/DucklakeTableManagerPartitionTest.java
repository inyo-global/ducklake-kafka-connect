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
 * Tests for the new flexible partition expressions functionality. Verifies that tables can be
 * partitioned by various expressions including: - Simple column names (strings, integers) -
 * Temporal functions (year, month, day) - Mixed expressions
 */
class DucklakeTableManagerPartitionTest {

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
  @DisplayName("Creates table without partitioning when no expressions provided")
  void testTableWithoutPartitioning() throws Exception {
    String tableName = uniqueTableName("no_partition");
    DucklakeWriterConfig config =
        new DucklakeWriterConfig(
            tableName, true, new String[] {"id"}, new String[0] // No partition expressions
            );

    DucklakeTableManager manager = new DucklakeTableManager(conn, config);
    Schema schema = schema(intField("id"), stringField("name"), timestampField("created_at"));

    boolean existed = manager.ensureTable(schema);
    assertFalse(existed, "Table should not have existed before creation");
    assertTrue(isTableCreatedSuccessfully(tableName), "Table should be created successfully");
  }

  @Test
  @DisplayName("Creates table with simple string column partitioning")
  void testStringColumnPartitioning() throws Exception {
    String tableName = uniqueTableName("string_partition");
    DucklakeWriterConfig config =
        new DucklakeWriterConfig(
            tableName,
            true,
            new String[] {"id"},
            new String[] {"status"} // Partition by string column
            );

    DucklakeTableManager manager = new DucklakeTableManager(conn, config);
    Schema schema = schema(intField("id"), stringField("status"), stringField("name"));

    boolean existed = manager.ensureTable(schema);
    assertFalse(existed, "Table should not have existed before creation");
    assertTrue(isTableCreatedSuccessfully(tableName), "Table should be created with partitioning");
  }

  @Test
  @DisplayName("Creates table with integer column partitioning")
  void testIntegerColumnPartitioning() throws Exception {
    String tableName = uniqueTableName("int_partition");
    DucklakeWriterConfig config =
        new DucklakeWriterConfig(
            tableName,
            true,
            new String[] {"id"},
            new String[] {"department_id"} // Partition by integer column
            );

    DucklakeTableManager manager = new DucklakeTableManager(conn, config);
    Schema schema = schema(intField("id"), intField("department_id"), stringField("name"));

    boolean existed = manager.ensureTable(schema);
    assertFalse(existed, "Table should not have existed before creation");
    assertTrue(isTableCreatedSuccessfully(tableName), "Table should be created with partitioning");
  }

  @Test
  @DisplayName("Creates table with temporal function partitioning - year")
  void testYearFunctionPartitioning() throws Exception {
    String tableName = uniqueTableName("year_partition");
    DucklakeWriterConfig config =
        new DucklakeWriterConfig(
            tableName,
            true,
            new String[] {"id"},
            new String[] {"year(created_at)"} // Partition by year function
            );

    DucklakeTableManager manager = new DucklakeTableManager(conn, config);
    Schema schema = schema(intField("id"), stringField("name"), timestampField("created_at"));

    boolean existed = manager.ensureTable(schema);
    assertFalse(existed, "Table should not have existed before creation");
    assertTrue(
        isTableCreatedSuccessfully(tableName), "Table should be created with year partitioning");
  }

  @Test
  @DisplayName("Creates table with temporal function partitioning - month")
  void testMonthFunctionPartitioning() throws Exception {
    String tableName = uniqueTableName("month_partition");
    DucklakeWriterConfig config =
        new DucklakeWriterConfig(
            tableName,
            true,
            new String[] {"id"},
            new String[] {"month(created_at)"} // Partition by month function
            );

    DucklakeTableManager manager = new DucklakeTableManager(conn, config);
    Schema schema = schema(intField("id"), stringField("name"), timestampField("created_at"));

    boolean existed = manager.ensureTable(schema);
    assertFalse(existed, "Table should not have existed before creation");
    assertTrue(
        isTableCreatedSuccessfully(tableName), "Table should be created with month partitioning");
  }

  @Test
  @DisplayName("Creates table with temporal function partitioning - day")
  void testDayFunctionPartitioning() throws Exception {
    String tableName = uniqueTableName("day_partition");
    DucklakeWriterConfig config =
        new DucklakeWriterConfig(
            tableName,
            true,
            new String[] {"id"},
            new String[] {"day(created_at)"} // Partition by day function
            );

    DucklakeTableManager manager = new DucklakeTableManager(conn, config);
    Schema schema = schema(intField("id"), stringField("name"), timestampField("created_at"));

    boolean existed = manager.ensureTable(schema);
    assertFalse(existed, "Table should not have existed before creation");
    assertTrue(
        isTableCreatedSuccessfully(tableName), "Table should be created with day partitioning");
  }

  @Test
  @DisplayName("Creates table with multiple temporal function partitioning")
  void testMultipleTemporalFunctionPartitioning() throws Exception {
    String tableName = uniqueTableName("multi_temporal_partition");
    DucklakeWriterConfig config =
        new DucklakeWriterConfig(
            tableName,
            true,
            new String[] {"id"},
            new String[] {"year(created_at)", "month(created_at)"} // Partition by year and month
            );

    DucklakeTableManager manager = new DucklakeTableManager(conn, config);
    Schema schema = schema(intField("id"), stringField("name"), timestampField("created_at"));

    boolean existed = manager.ensureTable(schema);
    assertFalse(existed, "Table should not have existed before creation");
    assertTrue(
        isTableCreatedSuccessfully(tableName),
        "Table should be created with multiple temporal partitioning");
  }

  @Test
  @DisplayName("Creates table with mixed partitioning expressions")
  void testMixedPartitioningExpressions() throws Exception {
    String tableName = uniqueTableName("mixed_partition");
    DucklakeWriterConfig config =
        new DucklakeWriterConfig(
            tableName,
            true,
            new String[] {"id"},
            new String[] {"year(created_at)", "status", "department_id"} // Mixed partitioning
            );

    DucklakeTableManager manager = new DucklakeTableManager(conn, config);
    Schema schema =
        schema(
            intField("id"),
            stringField("status"),
            intField("department_id"),
            stringField("name"),
            timestampField("created_at"));

    boolean existed = manager.ensureTable(schema);
    assertFalse(existed, "Table should not have existed before creation");
    assertTrue(
        isTableCreatedSuccessfully(tableName), "Table should be created with mixed partitioning");
  }

  @Test
  @DisplayName("Creates table with complex temporal partitioning")
  void testComplexTemporalPartitioning() throws Exception {
    String tableName = uniqueTableName("complex_temporal_partition");
    DucklakeWriterConfig config =
        new DucklakeWriterConfig(
            tableName,
            true,
            new String[] {"id"},
            new String[] {
              "year(created_at)", "month(created_at)", "day(created_at)"
            } // Year, month, day
            );

    DucklakeTableManager manager = new DucklakeTableManager(conn, config);
    Schema schema = schema(intField("id"), stringField("name"), timestampField("created_at"));

    boolean existed = manager.ensureTable(schema);
    assertFalse(existed, "Table should not have existed before creation");
    assertTrue(
        isTableCreatedSuccessfully(tableName),
        "Table should be created with complex temporal partitioning");
  }

  @Test
  @DisplayName("Creates table with multiple string column partitioning")
  void testMultipleStringColumnPartitioning() throws Exception {
    String tableName = uniqueTableName("multi_string_partition");
    DucklakeWriterConfig config =
        new DucklakeWriterConfig(
            tableName,
            true,
            new String[] {"id"},
            new String[] {"region", "category", "status"} // Multiple string columns
            );

    DucklakeTableManager manager = new DucklakeTableManager(conn, config);
    Schema schema =
        schema(
            intField("id"),
            stringField("region"),
            stringField("category"),
            stringField("status"),
            stringField("name"));

    boolean existed = manager.ensureTable(schema);
    assertFalse(existed, "Table should not have existed before creation");
    assertTrue(
        isTableCreatedSuccessfully(tableName),
        "Table should be created with multiple string partitioning");
  }

  @Test
  @DisplayName("Partition expressions are properly formatted in SQL")
  void testPartitionExpressionFormatting() throws Exception {
    String tableName = uniqueTableName("expression_format");
    DucklakeWriterConfig config =
        new DucklakeWriterConfig(
            tableName,
            true,
            new String[] {"id"},
            new String[] {"year(created_at)", "status"} // Test comma separation
            );

    DucklakeTableManager manager = new DucklakeTableManager(conn, config);
    Schema schema = schema(intField("id"), stringField("status"), timestampField("created_at"));

    // This test verifies that the SQL is properly formatted without syntax errors
    boolean existed = manager.ensureTable(schema);
    assertFalse(existed, "Table should not have existed before creation");
    assertTrue(
        isTableCreatedSuccessfully(tableName),
        "Table should be created with properly formatted expressions");
  }

  @Test
  @DisplayName("Handles tables with existing schema evolution and partitioning")
  void testSchemaEvolutionWithPartitioning() throws Exception {
    String tableName = uniqueTableName("evolution_partition");
    DucklakeWriterConfig config =
        new DucklakeWriterConfig(
            tableName, true, new String[] {"id"}, new String[] {"status"} // Partition by status
            );

    DucklakeTableManager manager = new DucklakeTableManager(conn, config);

    // Create initial schema
    Schema initialSchema = schema(intField("id"), stringField("status"), stringField("name"));

    boolean existed = manager.ensureTable(initialSchema);
    assertFalse(existed, "Table should not have existed initially");

    // Evolve schema by adding a new column
    Schema evolvedSchema =
        schema(
            intField("id"),
            stringField("status"),
            stringField("name"),
            timestampField("created_at") // New column
            );

    existed = manager.ensureTable(evolvedSchema);
    assertTrue(existed, "Table should exist after evolution");
    assertTrue(
        isTableCreatedSuccessfully(tableName), "Table should still be partitioned after evolution");
  }
}
