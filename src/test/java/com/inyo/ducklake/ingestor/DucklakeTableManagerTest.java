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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DucklakeTableManagerTest {

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

  private Schema schema(Field... fields) {
    return new Schema(Arrays.asList(fields));
  }

  private Field intField(String name, int bits) {
    return new Field(name, new FieldType(false, new ArrowType.Int(bits, true), null), null);
  }

  private Field floatField(String name, boolean doublePrecision) {
    return new Field(
        name,
        new FieldType(
            false,
            new ArrowType.FloatingPoint(
                doublePrecision ? FloatingPointPrecision.DOUBLE : FloatingPointPrecision.SINGLE),
            null),
        null);
  }

  private Field stringField(String name) {
    return new Field(name, new FieldType(false, ArrowType.Utf8.INSTANCE, null), null);
  }

  private Field structField(String name, Field... children) {
    return new Field(
        name, new FieldType(false, ArrowType.Struct.INSTANCE, null), Arrays.asList(children));
  }

  private Field listField(String name, Field elementField) {
    return new Field(
        name, new FieldType(false, ArrowType.List.INSTANCE, null), List.of(elementField));
  }

  private Field mapField(String name, Field keyField, Field valueField) {
    return new Field(
        name, new FieldType(false, new ArrowType.Map(true), null), List.of(keyField, valueField));
  }

  @Test
  @DisplayName("Creates table when absent and autoCreate=true")
  void testCreateTableAuto() throws Exception {
    String tableName = uniqueTableName("t1");
    DucklakeWriterConfig cfg =
        new DucklakeWriterConfig(tableName, true, new String[] {"id"}, new String[0]);
    DucklakeTableManager mgr = new DucklakeTableManager(conn, cfg);
    Schema s = schema(intField("id", 32), stringField("name"));
    mgr.ensureTable(s);

    // Verify existence via DuckDB PRAGMA in catalog 'lake'
    try (PreparedStatement ps =
        conn.prepareStatement("SELECT COUNT(*) FROM pragma_table_info(?)")) {
      ps.setString(1, "lake.main." + SqlIdentifierUtil.quote(tableName));
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
      }
    }
  }

  @Test
  @DisplayName("Fails if table does not exist and autoCreate=false")
  void testCreateTableDenied() {
    String tableName = uniqueTableName("t2");
    DucklakeWriterConfig cfg =
        new DucklakeWriterConfig(tableName, false, new String[] {}, new String[0]);
    DucklakeTableManager mgr = new DucklakeTableManager(conn, cfg);
    Schema s = schema(intField("a", 32));
    IllegalStateException ex = assertThrows(IllegalStateException.class, () -> mgr.ensureTable(s));
    assertTrue(ex.getMessage().contains("auto-create"));
  }

  @Test
  @DisplayName("Adds new column during evolution")
  void testAddNewColumnEvolution() throws Exception {
    String tableName = uniqueTableName("t3");
    DucklakeWriterConfig cfg =
        new DucklakeWriterConfig(tableName, true, new String[] {}, new String[0]);
    DucklakeTableManager mgr = new DucklakeTableManager(conn, cfg);
    mgr.ensureTable(schema(intField("a", 32)));
    // Evolve adding new column
    mgr.ensureTable(schema(intField("a", 32), stringField("b")));

    Set<String> cols = getColumns(tableName);
    assertEquals(Set.of("a", "b"), cols);
  }

  @Test
  @DisplayName("Accepts integer width promotion (existing wider)")
  void testIntegerPromotion() throws Exception {
    String tableName = uniqueTableName("t4");
    DucklakeWriterConfig cfg =
        new DucklakeWriterConfig(tableName, true, new String[] {}, new String[0]);
    DucklakeTableManager mgr = new DucklakeTableManager(conn, cfg);
    // create with BIGINT
    mgr.ensureTable(schema(intField("num", 64)));
    // new definition with INT32 should be accepted
    mgr.ensureTable(schema(intField("num", 32)));
    // still only one column
    assertEquals(Set.of("num"), getColumns(tableName));
  }

  @Test
  @DisplayName("Rejects incompatible type")
  void testIncompatibleType() throws Exception {
    String tableName = uniqueTableName("t5");
    DucklakeWriterConfig cfg =
        new DucklakeWriterConfig(tableName, true, new String[] {}, new String[0]);
    DucklakeTableManager mgr = new DucklakeTableManager(conn, cfg);
    // create with VARCHAR
    mgr.ensureTable(schema(stringField("c")));
    // attempt to evolve to INT -> should fail
    IllegalStateException ex =
        assertThrows(IllegalStateException.class, () -> mgr.ensureTable(schema(intField("c", 32))));
    assertTrue(ex.getMessage().contains("Incompatible type"));
  }

  @Test
  @DisplayName("Does not add duplicate column")
  void testNoDuplicateAdd() throws Exception {
    String tableName = uniqueTableName("t6");
    DucklakeWriterConfig cfg =
        new DucklakeWriterConfig(tableName, true, new String[] {}, new String[0]);
    DucklakeTableManager mgr = new DucklakeTableManager(conn, cfg);
    mgr.ensureTable(schema(intField("x", 32), stringField("y")));
    // same definition again
    mgr.ensureTable(schema(intField("x", 32), stringField("y")));
    assertEquals(Set.of("x", "y"), getColumns(tableName));
  }

  @Test
  @DisplayName("Accepts expected FLOAT when existing is DOUBLE")
  void testFloatDoubleCompatibility() throws Exception {
    String tableName = uniqueTableName("t7");
    DucklakeWriterConfig cfg =
        new DucklakeWriterConfig(tableName, true, new String[] {}, new String[0]);
    DucklakeTableManager mgr = new DucklakeTableManager(conn, cfg);
    mgr.ensureTable(schema(floatField("v", true))); // DOUBLE
    mgr.ensureTable(schema(floatField("v", false))); // expected FLOAT
    assertEquals(Set.of("v"), getColumns(tableName));
  }

  @Test
  @DisplayName("Creates JSON column for STRUCT field")
  void testStructCreatesJson() throws Exception {
    String tableName = uniqueTableName("t_struct");
    DucklakeWriterConfig cfg =
        new DucklakeWriterConfig(tableName, true, new String[] {}, new String[0]);
    DucklakeTableManager mgr = new DucklakeTableManager(conn, cfg);
    Field child = stringField("name");
    Field struct = structField("payload", child);
    mgr.ensureTable(schema(struct));
    assertColumnType(tableName, "payload", "JSON");
  }

  @Test
  @DisplayName("Creates JSON column for LIST field")
  void testListCreatesJson() throws Exception {
    String tableName = uniqueTableName("t_list");
    DucklakeWriterConfig cfg =
        new DucklakeWriterConfig(tableName, true, new String[] {}, new String[0]);
    DucklakeTableManager mgr = new DucklakeTableManager(conn, cfg);
    Field element = stringField("element");
    Field list = listField("tags", element);
    mgr.ensureTable(schema(list));
    assertColumnType(tableName, "tags", "JSON");
  }

  @Test
  @DisplayName("Creates JSON column for MAP field")
  void testMapCreatesJson() throws Exception {
    String tableName = uniqueTableName("t_map");
    DucklakeWriterConfig cfg =
        new DucklakeWriterConfig(tableName, true, new String[] {}, new String[0]);
    DucklakeTableManager mgr = new DucklakeTableManager(conn, cfg);
    Field key = stringField("key");
    Field value = stringField("value");
    Field map = mapField("attributes", key, value);
    mgr.ensureTable(schema(map));
    assertColumnType(tableName, "attributes", "JSON");
  }

  @Test
  @DisplayName("Evolves adding new JSON column from LIST")
  void testAddJsonColumnEvolution() throws Exception {
    String tableName = uniqueTableName("t_json_evo");
    DucklakeWriterConfig cfg =
        new DucklakeWriterConfig(tableName, true, new String[] {}, new String[0]);
    DucklakeTableManager mgr = new DucklakeTableManager(conn, cfg);
    mgr.ensureTable(schema(stringField("id")));
    Field element = stringField("element");
    Field list = listField("items", element);
    mgr.ensureTable(schema(stringField("id"), list));
    assertEquals(Set.of("id", "items"), getColumns(tableName));
    assertColumnType(tableName, "items", "JSON");
  }

  @Test
  @DisplayName("Upgrades INTEGER to BIGINT on evolution")
  void testIntegerTypeUpgrade() throws Exception {
    String tableName = uniqueTableName("t_upgrade_int");
    DucklakeWriterConfig cfg =
        new DucklakeWriterConfig(tableName, true, new String[] {}, new String[0]);
    DucklakeTableManager mgr = new DucklakeTableManager(conn, cfg);
    // create with INTEGER
    mgr.ensureTable(schema(intField("num", 32)));
    assertColumnType(tableName, "num", "INTEGER");
    // evolve to BIGINT
    mgr.ensureTable(schema(intField("num", 64)));
    assertColumnType(tableName, "num", "BIGINT");
  }

  @Test
  @DisplayName("Upgrades FLOAT to DOUBLE on evolution")
  void testFloatTypeUpgrade() throws Exception {
    String tableName = uniqueTableName("t_upgrade_float");
    DucklakeWriterConfig cfg =
        new DucklakeWriterConfig(tableName, true, new String[] {}, new String[0]);
    DucklakeTableManager mgr = new DucklakeTableManager(conn, cfg);
    // create with FLOAT
    mgr.ensureTable(schema(floatField("v", false))); // FLOAT
    assertColumnType(tableName, "v", "FLOAT");
    // evolve to DOUBLE
    mgr.ensureTable(schema(floatField("v", true))); // DOUBLE
    assertColumnType(tableName, "v", "DOUBLE");
  }

  @Test
  @DisplayName("Does not downgrade BIGINT to INTEGER")
  void testNoIntegerDowngrade() throws Exception {
    String tableName = uniqueTableName("t_no_down_int");
    DucklakeWriterConfig cfg =
        new DucklakeWriterConfig(tableName, true, new String[] {}, new String[0]);
    DucklakeTableManager mgr = new DucklakeTableManager(conn, cfg);
    mgr.ensureTable(schema(intField("num", 64))); // BIGINT
    assertColumnType(tableName, "num", "BIGINT");
    // attempt to downgrade to INTEGER
    mgr.ensureTable(schema(intField("num", 32))); // should keep BIGINT
    assertColumnType(tableName, "num", "BIGINT");
  }

  @Test
  @DisplayName("Does not downgrade DOUBLE to FLOAT")
  void testNoFloatDowngrade() throws Exception {
    String tableName = uniqueTableName("t_no_down_float");
    DucklakeWriterConfig cfg =
        new DucklakeWriterConfig(tableName, true, new String[] {}, new String[0]);
    DucklakeTableManager mgr = new DucklakeTableManager(conn, cfg);
    mgr.ensureTable(schema(floatField("v", true))); // DOUBLE
    assertColumnType(tableName, "v", "DOUBLE");
    // attempt to downgrade to FLOAT
    mgr.ensureTable(schema(floatField("v", false))); // should keep DOUBLE
    assertColumnType(tableName, "v", "DOUBLE");
  }

  @Test
  @DisplayName("Rejects evolution from JSON (STRUCT) to VARCHAR")
  void testJsonToVarcharIncompatible() throws Exception {
    String tableName = uniqueTableName("t_json_incompat");
    DucklakeWriterConfig cfg =
        new DucklakeWriterConfig(tableName, true, new String[] {}, new String[0]);
    DucklakeTableManager mgr = new DucklakeTableManager(conn, cfg);
    // initial STRUCT -> JSON
    Field struct = structField("payload", stringField("x"));
    mgr.ensureTable(schema(struct));
    assertColumnType(tableName, "payload", "JSON");
    // attempt to change to VARCHAR
    IllegalStateException ex =
        assertThrows(
            IllegalStateException.class, () -> mgr.ensureTable(schema(stringField("payload"))));
    assertTrue(ex.getMessage().contains("Incompatible type"));
  }

  private Set<String> getColumns(String table) throws Exception {
    Set<String> set = new LinkedHashSet<>();
    try (PreparedStatement ps = conn.prepareStatement("SELECT name FROM pragma_table_info(?)")) {
      ps.setString(1, "lake.main." + SqlIdentifierUtil.quote(table));
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          set.add(rs.getString("name"));
        }
      }
    }
    return set;
  }

  private void assertColumnType(String table, String column, String expectedType) throws Exception {
    try (PreparedStatement ps =
        conn.prepareStatement("SELECT name, type FROM pragma_table_info(?)")) {
      ps.setString(1, "lake.main." + SqlIdentifierUtil.quote(table));
      try (ResultSet rs = ps.executeQuery()) {
        boolean found = false;
        while (rs.next()) {
          if (rs.getString("name").equals(column)) {
            found = true;
            assertEquals(expectedType, rs.getString("type"));
          }
        }
        assertTrue(found, "Column not found: " + column);
      }
    }
  }

  /**
   * Generates a unique table name for each test to avoid conflicts when running tests multiple
   * times.
   */
  private String uniqueTableName(String prefix) {
    return prefix + "_" + UUID.randomUUID().toString().replace("-", "_");
  }
}
