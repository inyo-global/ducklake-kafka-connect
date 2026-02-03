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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.inyo.ducklake.TestHelper;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DucklakeWriterMergeTest {

  DuckDBConnection conn;

  @BeforeEach
  void setup() throws Exception {
    conn = TestHelper.setupDucklakeConnection();
  }

  @AfterEach
  void tearDown() throws Exception {
    conn.close();
  }

  private VectorSchemaRoot root(int[] ids, String[] names) {
    RootAllocator allocator = new RootAllocator();
    Schema schema = getSchema1();
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    IntVector idVec = (IntVector) root.getVector("id");
    VarCharVector nameVec = (VarCharVector) root.getVector("name");
    TimeStampMilliVector timestampVec = (TimeStampMilliVector) root.getVector("created_at");
    idVec.allocateNew(ids.length);
    nameVec.allocateNew();
    timestampVec.allocateNew(ids.length);

    long timestampMillis = Timestamp.valueOf(LocalDateTime.parse("2025-09-25T18:05:12")).getTime();

    for (int i = 0; i < ids.length; i++) {
      idVec.setSafe(i, ids[i]);
      byte[] bytes = names[i].getBytes(StandardCharsets.UTF_8);
      nameVec.setSafe(i, bytes, 0, bytes.length);
      timestampVec.setSafe(i, timestampMillis);
    }
    root.setRowCount(ids.length);
    return root;
  }

  @NotNull
  private static Schema getSchema1() {
    Field fId = new Field("id", new FieldType(false, new ArrowType.Int(32, true), null), null);
    Field fName = new Field("name", new FieldType(false, ArrowType.Utf8.INSTANCE, null), null);
    Field fTimestamp =
        new Field(
            "created_at",
            new FieldType(
                false,
                new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, null),
                null),
            null);
    return new Schema(List.of(fId, fName, fTimestamp));
  }

  private int tableCount() throws Exception {
    try (PreparedStatement ps =
        conn.prepareStatement(
            "SELECT COUNT(*) FROM lake.main." + SqlIdentifierUtil.quote("t_upsert"))) {
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    }
  }

  private String nameForId(int id) throws Exception {
    try (PreparedStatement ps =
        conn.prepareStatement(
            "SELECT name FROM lake.main." + SqlIdentifierUtil.quote("t_upsert") + " WHERE id=?")) {
      ps.setInt(1, id);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? rs.getString(1) : null;
      }
    }
  }

  @Test
  @DisplayName("Upsert via MERGE updates existing rows and inserts new ones")
  void testMergeUpsert() throws Exception {
    DucklakeWriterConfig cfg =
        new DucklakeWriterConfig(
            "t_upsert", true, new String[] {"id"}, new String[] {"created_at"});
    try (DucklakeWriter writer = new DucklakeWriter(conn, cfg)) {
      // Insert initial row
      try (VectorSchemaRoot r1 = root(new int[] {1}, new String[] {"alice"})) {
        writer.write(r1);
      }
      assertEquals(1, tableCount());
      assertEquals("alice", nameForId(1));

      // Upsert same PK with new name
      try (VectorSchemaRoot r2 = root(new int[] {1}, new String[] {"bob"})) {
        writer.write(r2);
      }
      assertEquals(1, tableCount(), "Row should be updated, not duplicated");
      assertEquals("bob", nameForId(1));

      // Batch with existing PK (update again) and new PK (insert)
      try (VectorSchemaRoot r3 = root(new int[] {1, 2}, new String[] {"carol", "dave"})) {
        writer.write(r3);
      }
      assertEquals(2, tableCount());
      assertEquals("carol", nameForId(1));
      assertEquals("dave", nameForId(2));
    }
  }

  @Test
  @DisplayName("MERGE positional mismatch should fail when inserting string into JSON column")
  void testMergePositionalMismatchThrows() throws Exception {
    // This test reproduces the case where an INSERT VALUES (positional) during MERGE
    // places an id string into a JSON column (address) because the source columns
    // are in a different order than the target table. We exercise the DucklakeWriter
    // so the production code path is tested.
    try (var stmt = conn.createStatement()) {
      // create target table with JSON first in lake.main so the writer's qualified name matches
      stmt.execute(
          "CREATE TABLE IF NOT EXISTS lake.main.batch_transaction_input_v7 (address JSON, id VARCHAR)");

      // Prepare a DucklakeWriter pointed at that table (table already exists so writer will use
      // MERGE)
      var cfg =
          new DucklakeWriterConfig(
              "batch_transaction_input_v7", true, new String[] {"id"}, new String[] {});
      try (var writer = new DucklakeWriter(conn, cfg)) {
        // Build a VectorSchemaRoot whose field order is (id, address) to simulate the positional
        // mismatch
        try (var root = rootWithIdAndAddress()) {
          // After the fix, this should not throw and should insert the row correctly
          writer.write(root);

          // Verify the row was inserted: id should match and address should be present as JSON
          try (var verifyStmt = conn.createStatement();
              var rs =
                  verifyStmt.executeQuery(
                      "SELECT id, address FROM lake.main.batch_transaction_input_v7")) {
            assertTrue(rs.next(), "Expected one row in target table");
            var storedId = rs.getString(1);
            var storedAddress = rs.getString(2);
            assertEquals("b11e2810-661b-49e5-ba8b-3a2e85bb5a7a", storedId);
            assertNotNull(storedAddress);
            assertTrue(
                storedAddress.contains("street"), "Address JSON should contain street field");
          }
        }
      }
    } finally {
      try (var cleanup = conn.createStatement()) {
        cleanup.execute("DROP TABLE IF EXISTS lake.main.batch_transaction_input_v7");
      } catch (java.sql.SQLException ignored) {
      }
    }
  }

  // Helper which creates an Arrow VectorSchemaRoot with columns in order: id, address
  private VectorSchemaRoot rootWithIdAndAddress() {
    var allocator = new RootAllocator();
    var schema = getSchema();
    var root = VectorSchemaRoot.create(schema, allocator);

    var idVec = (VarCharVector) root.getVector("id");
    var addressStruct = (org.apache.arrow.vector.complex.StructVector) root.getVector("address");
    idVec.allocateNew();
    addressStruct.allocateNew();

    var idBytes =
        "b11e2810-661b-49e5-ba8b-3a2e85bb5a7a".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    idVec.setSafe(0, idBytes, 0, idBytes.length);

    // set child street field inside the struct
    var streetVec = (VarCharVector) addressStruct.getChild("street");
    streetVec.allocateNew();
    var streetBytes = "x".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    streetVec.setSafe(0, streetBytes, 0, streetBytes.length);
    addressStruct.setIndexDefined(0);

    root.setRowCount(1);
    return root;
  }

  @Test
  @DisplayName("_inserted_at column is populated on simple insert")
  void testInsertedAtPopulatedOnInsert() throws Exception {
    String tableName =
        "t_inserted_at_insert_" + java.util.UUID.randomUUID().toString().replace("-", "_");
    DucklakeWriterConfig cfg =
        new DucklakeWriterConfig(tableName, true, new String[] {}, new String[] {});
    try (DucklakeWriter writer = new DucklakeWriter(conn, cfg)) {
      try (VectorSchemaRoot r1 = root(new int[] {1, 2}, new String[] {"alice", "bob"})) {
        writer.write(r1);
      }

      // Verify _inserted_at is populated for both rows
      try (var ps =
          conn.prepareStatement(
              "SELECT id, "
                  + SqlIdentifierUtil.quote(DucklakeTableManager.INSERTED_AT_COLUMN)
                  + " FROM lake.main."
                  + SqlIdentifierUtil.quote(tableName)
                  + " ORDER BY id")) {
        try (var rs = ps.executeQuery()) {
          assertTrue(rs.next(), "Should have first row");
          assertEquals(1, rs.getInt(1));
          assertNotNull(rs.getTimestamp(2), "_inserted_at should be set for row 1");

          assertTrue(rs.next(), "Should have second row");
          assertEquals(2, rs.getInt(1));
          assertNotNull(rs.getTimestamp(2), "_inserted_at should be set for row 2");
        }
      }
    }
  }

  @Test
  @DisplayName("_inserted_at column is populated on MERGE insert (WHEN NOT MATCHED)")
  void testInsertedAtPopulatedOnMergeInsert() throws Exception {
    String tableName =
        "t_inserted_at_merge_insert_" + java.util.UUID.randomUUID().toString().replace("-", "_");
    DucklakeWriterConfig cfg =
        new DucklakeWriterConfig(tableName, true, new String[] {"id"}, new String[] {"created_at"});
    try (DucklakeWriter writer = new DucklakeWriter(conn, cfg)) {
      // First write creates the table (uses simple INSERT for new tables)
      try (VectorSchemaRoot r1 = root(new int[] {1}, new String[] {"alice"})) {
        writer.write(r1);
      }

      // Second write uses MERGE since table exists; id=2 will go through WHEN NOT MATCHED
      try (VectorSchemaRoot r2 = root(new int[] {2}, new String[] {"bob"})) {
        writer.write(r2);
      }

      // Verify _inserted_at is populated for the MERGE-inserted row
      try (var ps =
          conn.prepareStatement(
              "SELECT "
                  + SqlIdentifierUtil.quote(DucklakeTableManager.INSERTED_AT_COLUMN)
                  + " FROM lake.main."
                  + SqlIdentifierUtil.quote(tableName)
                  + " WHERE id = 2")) {
        try (var rs = ps.executeQuery()) {
          assertTrue(rs.next(), "Should have row inserted via MERGE");
          assertNotNull(rs.getTimestamp(1), "_inserted_at should be set for MERGE-inserted row");
        }
      }
    }
  }

  @Test
  @DisplayName("_inserted_at column is NOT updated on MERGE update")
  void testInsertedAtNotUpdatedOnMerge() throws Exception {
    String tableName =
        "t_inserted_at_merge_" + java.util.UUID.randomUUID().toString().replace("-", "_");
    DucklakeWriterConfig cfg =
        new DucklakeWriterConfig(tableName, true, new String[] {"id"}, new String[] {"created_at"});
    try (DucklakeWriter writer = new DucklakeWriter(conn, cfg)) {
      // Initial insert
      try (VectorSchemaRoot r1 = root(new int[] {1}, new String[] {"alice"})) {
        writer.write(r1);
      }

      // Get the original _inserted_at timestamp
      Timestamp originalInsertedAt;
      try (var ps =
          conn.prepareStatement(
              "SELECT "
                  + SqlIdentifierUtil.quote(DucklakeTableManager.INSERTED_AT_COLUMN)
                  + " FROM lake.main."
                  + SqlIdentifierUtil.quote(tableName)
                  + " WHERE id = 1")) {
        try (var rs = ps.executeQuery()) {
          assertTrue(rs.next(), "Should have row");
          originalInsertedAt = rs.getTimestamp(1);
          assertNotNull(originalInsertedAt, "_inserted_at should be set");
        }
      }

      // Wait a bit to ensure timestamp would differ if updated
      Thread.sleep(100);

      // Update via MERGE (same PK, different name)
      try (VectorSchemaRoot r2 = root(new int[] {1}, new String[] {"bob"})) {
        writer.write(r2);
      }

      // Verify name was updated but _inserted_at was NOT changed
      try (var ps =
          conn.prepareStatement(
              "SELECT name, "
                  + SqlIdentifierUtil.quote(DucklakeTableManager.INSERTED_AT_COLUMN)
                  + " FROM lake.main."
                  + SqlIdentifierUtil.quote(tableName)
                  + " WHERE id = 1")) {
        try (var rs = ps.executeQuery()) {
          assertTrue(rs.next(), "Should have row");
          assertEquals("bob", rs.getString(1), "Name should be updated");
          Timestamp afterUpdateInsertedAt = rs.getTimestamp(2);
          assertEquals(
              originalInsertedAt,
              afterUpdateInsertedAt,
              "_inserted_at should NOT change on MERGE update");
        }
      }
    }
  }

  @NotNull
  private static Schema getSchema() {
    var fId = new Field("id", new FieldType(false, ArrowType.Utf8.INSTANCE, null), null);
    // Create address as a struct with one child field 'street' so toDuckDBType maps it to JSON
    var streetField =
        new Field("street", new FieldType(false, ArrowType.Utf8.INSTANCE, null), null);
    var fAddress =
        new Field(
            "address", new FieldType(false, new ArrowType.Struct(), null), List.of(streetField));
    return new Schema(List.of(fId, fAddress));
  }
}
