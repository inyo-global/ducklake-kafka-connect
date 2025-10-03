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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
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

  private VectorSchemaRoot root(int[] ids, String[] names, String timestamp) {
    RootAllocator allocator = new RootAllocator();
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
    Schema schema = new Schema(List.of(fId, fName, fTimestamp));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    IntVector idVec = (IntVector) root.getVector("id");
    VarCharVector nameVec = (VarCharVector) root.getVector("name");
    TimeStampMilliVector timestampVec = (TimeStampMilliVector) root.getVector("created_at");
    idVec.allocateNew(ids.length);
    nameVec.allocateNew();
    timestampVec.allocateNew(ids.length);

    long timestampMillis = Timestamp.valueOf(LocalDateTime.parse(timestamp)).getTime();

    for (int i = 0; i < ids.length; i++) {
      idVec.setSafe(i, ids[i]);
      byte[] bytes = names[i].getBytes(StandardCharsets.UTF_8);
      nameVec.setSafe(i, bytes, 0, bytes.length);
      timestampVec.setSafe(i, timestampMillis);
    }
    root.setRowCount(ids.length);
    return root;
  }

  private int tableCount(String table) throws Exception {
    try (PreparedStatement ps =
        conn.prepareStatement("SELECT COUNT(*) FROM lake.main." + SqlIdentifierUtil.quote(table))) {
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    }
  }

  private String nameForId(String table, int id) throws Exception {
    try (PreparedStatement ps =
        conn.prepareStatement(
            "SELECT name FROM lake.main." + SqlIdentifierUtil.quote(table) + " WHERE id=?")) {
      ps.setInt(1, id);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? rs.getString(1) : null;
      }
    }
  }

  @Test
  @Disabled("Waiting ducklake version 1.4.1 that fix merge into with partition by")
  @DisplayName("Upsert via MERGE updates existing rows and inserts new ones")
  void testMergeUpsert() throws Exception {
    DucklakeWriterConfig cfg =
        new DucklakeWriterConfig(
            "t_upsert", true, new String[] {"id"}, new String[] {"created_at"});
    try (DucklakeWriter writer = new DucklakeWriter(conn, cfg)) {
      // Insert initial row
      try (VectorSchemaRoot r1 =
          root(new int[] {1}, new String[] {"alice"}, "2025-09-25T18:05:12")) {
        writer.write(r1);
      }
      assertEquals(1, tableCount("t_upsert"));
      assertEquals("alice", nameForId("t_upsert", 1));

      // Upsert same PK with new name
      try (VectorSchemaRoot r2 = root(new int[] {1}, new String[] {"bob"}, "2025-09-25T18:05:12")) {
        writer.write(r2);
      }
      assertEquals(1, tableCount("t_upsert"), "Row should be updated, not duplicated");
      assertEquals("bob", nameForId("t_upsert", 1));

      // Batch with existing PK (update again) and new PK (insert)
      try (VectorSchemaRoot r3 =
          root(new int[] {1, 2}, new String[] {"carol", "dave"}, "2025-09-25T18:05:12")) {
        writer.write(r3);
      }
      assertEquals(2, tableCount("t_upsert"));
      assertEquals("carol", nameForId("t_upsert", 1));
      assertEquals("dave", nameForId("t_upsert", 2));
    }
  }
}
