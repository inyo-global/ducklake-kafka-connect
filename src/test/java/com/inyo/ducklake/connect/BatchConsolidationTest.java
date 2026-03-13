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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class BatchConsolidationTest {

  private BufferAllocator allocator;

  private static final Schema INT_SCHEMA =
      new Schema(List.of(new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)));

  private static final Schema STRING_SCHEMA =
      new Schema(List.of(new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));

  private static final Schema BIGINT_SCHEMA =
      new Schema(List.of(new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null)));

  private static final Schema MULTI_COL_SCHEMA =
      new Schema(
          List.of(
              new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
              new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
              new Field(
                  "score",
                  FieldType.nullable(
                      new ArrowType.FloatingPoint(
                          org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)),
                  null)));

  @BeforeEach
  void setUp() {
    allocator = new RootAllocator();
  }

  @AfterEach
  void tearDown() {
    if (allocator != null) {
      allocator.close();
    }
  }

  // ---- Helper methods ----

  private VectorSchemaRoot createIntBatch(int... values) {
    VectorSchemaRoot root = VectorSchemaRoot.create(INT_SCHEMA, allocator);
    root.allocateNew();
    IntVector vec = (IntVector) root.getVector("id");
    for (int i = 0; i < values.length; i++) {
      vec.setSafe(i, values[i]);
    }
    root.setRowCount(values.length);
    return root;
  }

  private VectorSchemaRoot createIntBatchWithNulls(Integer... values) {
    VectorSchemaRoot root = VectorSchemaRoot.create(INT_SCHEMA, allocator);
    root.allocateNew();
    IntVector vec = (IntVector) root.getVector("id");
    for (int i = 0; i < values.length; i++) {
      if (values[i] == null) {
        vec.setNull(i);
      } else {
        vec.setSafe(i, values[i]);
      }
    }
    root.setRowCount(values.length);
    return root;
  }

  private VectorSchemaRoot createStringBatch(String... values) {
    VectorSchemaRoot root = VectorSchemaRoot.create(STRING_SCHEMA, allocator);
    root.allocateNew();
    VarCharVector vec = (VarCharVector) root.getVector("name");
    for (int i = 0; i < values.length; i++) {
      vec.setSafe(i, values[i].getBytes(StandardCharsets.UTF_8));
    }
    root.setRowCount(values.length);
    return root;
  }

  private VectorSchemaRoot createBigIntBatch(long... values) {
    VectorSchemaRoot root = VectorSchemaRoot.create(BIGINT_SCHEMA, allocator);
    root.allocateNew();
    BigIntVector vec = (BigIntVector) root.getVector("id");
    for (int i = 0; i < values.length; i++) {
      vec.setSafe(i, values[i]);
    }
    root.setRowCount(values.length);
    return root;
  }

  private VectorSchemaRoot createMultiColBatch(int[] ids, String[] names, double[] scores) {
    VectorSchemaRoot root = VectorSchemaRoot.create(MULTI_COL_SCHEMA, allocator);
    root.allocateNew();
    IntVector idVec = (IntVector) root.getVector("id");
    VarCharVector nameVec = (VarCharVector) root.getVector("name");
    Float8Vector scoreVec = (Float8Vector) root.getVector("score");
    for (int i = 0; i < ids.length; i++) {
      idVec.setSafe(i, ids[i]);
      nameVec.setSafe(i, names[i].getBytes(StandardCharsets.UTF_8));
      scoreVec.setSafe(i, scores[i]);
    }
    root.setRowCount(ids.length);
    return root;
  }

  private VectorSchemaRoot createEmptyBatch(Schema schema) {
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();
    root.setRowCount(0);
    return root;
  }

  private void assertIntValues(VectorSchemaRoot root, int... expected) {
    assertEquals(expected.length, root.getRowCount());
    IntVector vec = (IntVector) root.getVector("id");
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], vec.get(i), "Mismatch at row " + i);
    }
  }

  private void closeAll(List<VectorSchemaRoot> roots) {
    for (VectorSchemaRoot root : roots) {
      root.close();
    }
  }

  // ---- Tests ----

  @Nested
  @DisplayName("Same schema consolidation")
  class SameSchema {

    @Test
    void emptyList() {
      List<VectorSchemaRoot> result = BatchConsolidator.consolidate(Collections.emptyList());
      assertEquals(0, result.size());
    }

    @Test
    void singleBatch() {
      VectorSchemaRoot batch = createIntBatch(1, 2, 3);
      List<VectorSchemaRoot> result = BatchConsolidator.consolidate(List.of(batch));
      assertEquals(1, result.size());
      assertSame(batch, result.get(0));
      assertIntValues(result.get(0), 1, 2, 3);
      closeAll(result);
    }

    @Test
    void singleBatchZeroRows() {
      VectorSchemaRoot batch = createEmptyBatch(INT_SCHEMA);
      List<VectorSchemaRoot> result = BatchConsolidator.consolidate(List.of(batch));
      assertEquals(1, result.size());
      assertSame(batch, result.get(0));
      assertEquals(0, result.get(0).getRowCount());
      closeAll(result);
    }

    @Test
    void twoBatches() {
      VectorSchemaRoot a = createIntBatch(1, 2, 3);
      VectorSchemaRoot b = createIntBatch(4, 5);
      List<VectorSchemaRoot> result = BatchConsolidator.consolidate(List.of(a, b));
      assertEquals(1, result.size());
      assertEquals(5, result.get(0).getRowCount());
      assertIntValues(result.get(0), 1, 2, 3, 4, 5);
      closeAll(result);
    }

    @Test
    void manyBatches() {
      List<VectorSchemaRoot> batches = new ArrayList<>();
      int expectedTotal = 0;
      for (int i = 0; i < 4; i++) {
        int size = (i + 1) * 10;
        int[] values = new int[size];
        for (int j = 0; j < size; j++) {
          values[j] = expectedTotal + j;
        }
        batches.add(createIntBatch(values));
        expectedTotal += size;
      }
      List<VectorSchemaRoot> result = BatchConsolidator.consolidate(batches);
      assertEquals(1, result.size());
      assertEquals(expectedTotal, result.get(0).getRowCount());
      // Verify first and last values
      IntVector vec = (IntVector) result.get(0).getVector("id");
      assertEquals(0, vec.get(0));
      assertEquals(expectedTotal - 1, vec.get(expectedTotal - 1));
      closeAll(result);
    }

    @Test
    void dataIntegrityPreserved() {
      VectorSchemaRoot a = createIntBatch(10, 20, 30);
      VectorSchemaRoot b = createIntBatch(40, 50, 60);
      VectorSchemaRoot c = createIntBatch(70);
      List<VectorSchemaRoot> result = BatchConsolidator.consolidate(List.of(a, b, c));
      assertEquals(1, result.size());
      assertIntValues(result.get(0), 10, 20, 30, 40, 50, 60, 70);
      closeAll(result);
    }

    @Test
    void multipleColumnsPreserved() {
      VectorSchemaRoot a =
          createMultiColBatch(
              new int[] {1, 2}, new String[] {"alice", "bob"}, new double[] {1.0, 2.0});
      VectorSchemaRoot b =
          createMultiColBatch(new int[] {3}, new String[] {"charlie"}, new double[] {3.0});
      List<VectorSchemaRoot> result = BatchConsolidator.consolidate(List.of(a, b));
      assertEquals(1, result.size());
      VectorSchemaRoot r = result.get(0);
      assertEquals(3, r.getRowCount());

      IntVector idVec = (IntVector) r.getVector("id");
      VarCharVector nameVec = (VarCharVector) r.getVector("name");
      Float8Vector scoreVec = (Float8Vector) r.getVector("score");

      assertEquals(1, idVec.get(0));
      assertEquals(2, idVec.get(1));
      assertEquals(3, idVec.get(2));
      assertEquals("alice", new String(nameVec.get(0), StandardCharsets.UTF_8));
      assertEquals("bob", new String(nameVec.get(1), StandardCharsets.UTF_8));
      assertEquals("charlie", new String(nameVec.get(2), StandardCharsets.UTF_8));
      assertEquals(1.0, scoreVec.get(0));
      assertEquals(2.0, scoreVec.get(1));
      assertEquals(3.0, scoreVec.get(2));
      closeAll(result);
    }

    @Test
    void nullValuesPreserved() {
      VectorSchemaRoot a = createIntBatchWithNulls(1, null, 3);
      VectorSchemaRoot b = createIntBatchWithNulls(null, 5);
      List<VectorSchemaRoot> result = BatchConsolidator.consolidate(List.of(a, b));
      assertEquals(1, result.size());
      VectorSchemaRoot r = result.get(0);
      assertEquals(5, r.getRowCount());
      IntVector vec = (IntVector) r.getVector("id");
      assertFalse(vec.isNull(0));
      assertEquals(1, vec.get(0));
      assertTrue(vec.isNull(1));
      assertFalse(vec.isNull(2));
      assertEquals(3, vec.get(2));
      assertTrue(vec.isNull(3));
      assertFalse(vec.isNull(4));
      assertEquals(5, vec.get(4));
      closeAll(result);
    }

    @Test
    void allNullColumn() {
      VectorSchemaRoot a = createIntBatchWithNulls(null, null);
      VectorSchemaRoot b = createIntBatchWithNulls(1, 2);
      List<VectorSchemaRoot> result = BatchConsolidator.consolidate(List.of(a, b));
      assertEquals(1, result.size());
      VectorSchemaRoot r = result.get(0);
      assertEquals(4, r.getRowCount());
      IntVector vec = (IntVector) r.getVector("id");
      assertTrue(vec.isNull(0));
      assertTrue(vec.isNull(1));
      assertEquals(1, vec.get(2));
      assertEquals(2, vec.get(3));
      closeAll(result);
    }
  }

  @Nested
  @DisplayName("Different schema - contiguous run grouping")
  class DifferentSchemas {

    @Test
    void twoBatchesDifferentSchemas() {
      VectorSchemaRoot a = createIntBatch(1, 2);
      VectorSchemaRoot b = createStringBatch("x", "y");
      List<VectorSchemaRoot> result = BatchConsolidator.consolidate(List.of(a, b));
      assertEquals(2, result.size());
      assertEquals(2, result.get(0).getRowCount());
      assertEquals(2, result.get(1).getRowCount());
      closeAll(result);
    }

    @Test
    void oddballInMiddle() {
      VectorSchemaRoot a1 = createIntBatch(1, 2);
      VectorSchemaRoot a2 = createIntBatch(3, 4);
      VectorSchemaRoot b = createStringBatch("x");
      VectorSchemaRoot a3 = createIntBatch(5, 6);
      VectorSchemaRoot a4 = createIntBatch(7, 8);
      List<VectorSchemaRoot> result = BatchConsolidator.consolidate(List.of(a1, a2, b, a3, a4));
      assertEquals(3, result.size());
      // First run: two int batches consolidated
      assertIntValues(result.get(0), 1, 2, 3, 4);
      // Middle: string batch
      assertEquals(1, result.get(1).getRowCount());
      VarCharVector strVec = (VarCharVector) result.get(1).getVector("name");
      assertEquals("x", new String(strVec.get(0), StandardCharsets.UTF_8));
      // Last run: two int batches consolidated
      assertIntValues(result.get(2), 5, 6, 7, 8);
      closeAll(result);
    }

    @Test
    void alternatingSchemas() {
      VectorSchemaRoot a1 = createIntBatch(1);
      VectorSchemaRoot b1 = createStringBatch("x");
      VectorSchemaRoot a2 = createIntBatch(2);
      VectorSchemaRoot b2 = createStringBatch("y");
      List<VectorSchemaRoot> result = BatchConsolidator.consolidate(List.of(a1, b1, a2, b2));
      assertEquals(4, result.size());
      closeAll(result);
    }

    @Test
    void schemaChangeAtEnd() {
      VectorSchemaRoot a1 = createIntBatch(1, 2);
      VectorSchemaRoot a2 = createIntBatch(3, 4);
      VectorSchemaRoot a3 = createIntBatch(5);
      VectorSchemaRoot b = createStringBatch("x");
      List<VectorSchemaRoot> result = BatchConsolidator.consolidate(List.of(a1, a2, a3, b));
      assertEquals(2, result.size());
      assertIntValues(result.get(0), 1, 2, 3, 4, 5);
      assertEquals(1, result.get(1).getRowCount());
      closeAll(result);
    }

    @Test
    void schemaChangeAtStart() {
      VectorSchemaRoot b = createStringBatch("x");
      VectorSchemaRoot a1 = createIntBatch(1, 2);
      VectorSchemaRoot a2 = createIntBatch(3, 4);
      VectorSchemaRoot a3 = createIntBatch(5);
      List<VectorSchemaRoot> result = BatchConsolidator.consolidate(List.of(b, a1, a2, a3));
      assertEquals(2, result.size());
      assertEquals(1, result.get(0).getRowCount());
      assertIntValues(result.get(1), 1, 2, 3, 4, 5);
      closeAll(result);
    }

    @Test
    void threeDifferentSchemas() {
      VectorSchemaRoot a1 = createIntBatch(1);
      VectorSchemaRoot a2 = createIntBatch(2);
      VectorSchemaRoot b1 = createStringBatch("x");
      VectorSchemaRoot b2 = createStringBatch("y");
      VectorSchemaRoot c1 = createBigIntBatch(100L);
      VectorSchemaRoot c2 = createBigIntBatch(200L);
      List<VectorSchemaRoot> result =
          BatchConsolidator.consolidate(List.of(a1, a2, b1, b2, c1, c2));
      assertEquals(3, result.size());
      assertEquals(2, result.get(0).getRowCount());
      assertEquals(2, result.get(1).getRowCount());
      assertEquals(2, result.get(2).getRowCount());
      closeAll(result);
    }

    @Test
    void singleOddballAmongMany() {
      List<VectorSchemaRoot> batches = new ArrayList<>();
      for (int i = 0; i < 100; i++) {
        batches.add(createIntBatch(i));
      }
      batches.add(createStringBatch("oddball"));
      for (int i = 100; i < 200; i++) {
        batches.add(createIntBatch(i));
      }
      List<VectorSchemaRoot> result = BatchConsolidator.consolidate(batches);
      assertEquals(3, result.size());
      assertEquals(100, result.get(0).getRowCount());
      assertEquals(1, result.get(1).getRowCount());
      assertEquals(100, result.get(2).getRowCount());
      // Verify ordering of first run
      IntVector firstRun = (IntVector) result.get(0).getVector("id");
      assertEquals(0, firstRun.get(0));
      assertEquals(99, firstRun.get(99));
      // Verify ordering of last run
      IntVector lastRun = (IntVector) result.get(2).getVector("id");
      assertEquals(100, lastRun.get(0));
      assertEquals(199, lastRun.get(99));
      closeAll(result);
    }
  }

  @Nested
  @DisplayName("Ordering preservation")
  class Ordering {

    @Test
    void orderWithinRun() {
      VectorSchemaRoot a = createIntBatch(1, 2);
      VectorSchemaRoot b = createIntBatch(3, 4);
      List<VectorSchemaRoot> result = BatchConsolidator.consolidate(List.of(a, b));
      assertEquals(1, result.size());
      assertIntValues(result.get(0), 1, 2, 3, 4);
      closeAll(result);
    }

    @Test
    void orderAcrossRuns() {
      VectorSchemaRoot a1 = createIntBatch(1, 2);
      VectorSchemaRoot b = createStringBatch("mid");
      VectorSchemaRoot a2 = createIntBatch(3, 4);
      List<VectorSchemaRoot> result = BatchConsolidator.consolidate(List.of(a1, b, a2));
      assertEquals(3, result.size());
      assertIntValues(result.get(0), 1, 2);
      VarCharVector strVec = (VarCharVector) result.get(1).getVector("name");
      assertEquals("mid", new String(strVec.get(0), StandardCharsets.UTF_8));
      assertIntValues(result.get(2), 3, 4);
      closeAll(result);
    }
  }

  @Nested
  @DisplayName("Schema compatibility edge cases")
  class SchemaEdgeCases {

    @Test
    void differentNullability() {
      Schema nonNullable =
          new Schema(
              List.of(
                  new Field("id", new FieldType(false, new ArrowType.Int(32, true), null), null)));
      VectorSchemaRoot a = createIntBatch(1, 2);
      VectorSchemaRoot b = VectorSchemaRoot.create(nonNullable, allocator);
      b.allocateNew();
      ((IntVector) b.getVector("id")).setSafe(0, 3);
      b.setRowCount(1);

      List<VectorSchemaRoot> result = BatchConsolidator.consolidate(List.of(a, b));
      assertEquals(2, result.size());
      closeAll(result);
    }

    @Test
    void differentFieldNames() {
      Schema keySchema =
          new Schema(
              List.of(new Field("key", FieldType.nullable(new ArrowType.Int(32, true)), null)));
      VectorSchemaRoot a = createIntBatch(1);
      VectorSchemaRoot b = VectorSchemaRoot.create(keySchema, allocator);
      b.allocateNew();
      ((IntVector) b.getVector("key")).setSafe(0, 2);
      b.setRowCount(1);

      List<VectorSchemaRoot> result = BatchConsolidator.consolidate(List.of(a, b));
      assertEquals(2, result.size());
      closeAll(result);
    }

    @Test
    void differentFieldOrder() {
      Schema ab =
          new Schema(
              List.of(
                  new Field("a", FieldType.nullable(new ArrowType.Int(32, true)), null),
                  new Field("b", FieldType.nullable(new ArrowType.Utf8()), null)));
      Schema ba =
          new Schema(
              List.of(
                  new Field("b", FieldType.nullable(new ArrowType.Utf8()), null),
                  new Field("a", FieldType.nullable(new ArrowType.Int(32, true)), null)));
      VectorSchemaRoot batchAB = VectorSchemaRoot.create(ab, allocator);
      batchAB.allocateNew();
      ((IntVector) batchAB.getVector("a")).setSafe(0, 1);
      ((VarCharVector) batchAB.getVector("b")).setSafe(0, "x".getBytes(StandardCharsets.UTF_8));
      batchAB.setRowCount(1);

      VectorSchemaRoot batchBA = VectorSchemaRoot.create(ba, allocator);
      batchBA.allocateNew();
      ((VarCharVector) batchBA.getVector("b")).setSafe(0, "y".getBytes(StandardCharsets.UTF_8));
      ((IntVector) batchBA.getVector("a")).setSafe(0, 2);
      batchBA.setRowCount(1);

      List<VectorSchemaRoot> result = BatchConsolidator.consolidate(List.of(batchAB, batchBA));
      assertEquals(2, result.size());
      closeAll(result);
    }

    @Test
    void nestedStructSameSchema() {
      Schema structSchema =
          new Schema(
              List.of(
                  new Field(
                      "outer",
                      FieldType.nullable(new ArrowType.Struct()),
                      List.of(
                          new Field(
                              "inner", FieldType.nullable(new ArrowType.Int(32, true)), null)))));
      VectorSchemaRoot a = VectorSchemaRoot.create(structSchema, allocator);
      a.allocateNew();
      a.setRowCount(0);
      VectorSchemaRoot b = VectorSchemaRoot.create(structSchema, allocator);
      b.allocateNew();
      b.setRowCount(0);

      List<VectorSchemaRoot> result = BatchConsolidator.consolidate(List.of(a, b));
      // Same schema — should consolidate
      assertEquals(1, result.size());
      closeAll(result);
    }

    @Test
    void nestedStructDifferentChildTypes() {
      Schema structInt =
          new Schema(
              List.of(
                  new Field(
                      "outer",
                      FieldType.nullable(new ArrowType.Struct()),
                      List.of(
                          new Field(
                              "inner", FieldType.nullable(new ArrowType.Int(32, true)), null)))));
      Schema structStr =
          new Schema(
              List.of(
                  new Field(
                      "outer",
                      FieldType.nullable(new ArrowType.Struct()),
                      List.of(
                          new Field("inner", FieldType.nullable(new ArrowType.Utf8()), null)))));
      VectorSchemaRoot a = VectorSchemaRoot.create(structInt, allocator);
      a.allocateNew();
      a.setRowCount(0);
      VectorSchemaRoot b = VectorSchemaRoot.create(structStr, allocator);
      b.allocateNew();
      b.setRowCount(0);

      List<VectorSchemaRoot> result = BatchConsolidator.consolidate(List.of(a, b));
      assertEquals(2, result.size());
      closeAll(result);
    }

    @Test
    void listTypeSameElementType() {
      Schema listSchema =
          new Schema(
              List.of(
                  new Field(
                      "values",
                      FieldType.nullable(new ArrowType.List()),
                      List.of(
                          new Field(
                              "element", FieldType.nullable(new ArrowType.Int(32, true)), null)))));
      VectorSchemaRoot a = VectorSchemaRoot.create(listSchema, allocator);
      a.allocateNew();
      a.setRowCount(0);
      VectorSchemaRoot b = VectorSchemaRoot.create(listSchema, allocator);
      b.allocateNew();
      b.setRowCount(0);

      List<VectorSchemaRoot> result = BatchConsolidator.consolidate(List.of(a, b));
      assertEquals(1, result.size());
      closeAll(result);
    }

    @Test
    void sameFieldsDifferentMetadata() {
      Schema withMeta =
          new Schema(
              List.of(
                  new Field(
                      "id",
                      new FieldType(true, new ArrowType.Int(32, true), null, Map.of("key", "val")),
                      null)));
      Schema withoutMeta =
          new Schema(
              List.of(new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)));

      // schemasAreCompatible ignores metadata — these should consolidate
      assertTrue(BatchConsolidator.schemasAreCompatible(withMeta, withoutMeta));
    }

    @Test
    void differentFieldCount() {
      Schema twoFields =
          new Schema(
              List.of(
                  new Field("a", FieldType.nullable(new ArrowType.Int(32, true)), null),
                  new Field("b", FieldType.nullable(new ArrowType.Int(32, true)), null)));
      VectorSchemaRoot a = createIntBatch(1);
      VectorSchemaRoot b = VectorSchemaRoot.create(twoFields, allocator);
      b.allocateNew();
      ((IntVector) b.getVector("a")).setSafe(0, 1);
      ((IntVector) b.getVector("b")).setSafe(0, 2);
      b.setRowCount(1);

      List<VectorSchemaRoot> result = BatchConsolidator.consolidate(List.of(a, b));
      assertEquals(2, result.size());
      closeAll(result);
    }
  }

  @Nested
  @DisplayName("schemasAreCompatible")
  class SchemasAreCompatibleTests {

    @Test
    void identicalSchemas() {
      assertTrue(BatchConsolidator.schemasAreCompatible(INT_SCHEMA, INT_SCHEMA));
    }

    @Test
    void referenceEquality() {
      Schema s =
          new Schema(
              List.of(new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)));
      assertTrue(BatchConsolidator.schemasAreCompatible(s, s));
    }

    @Test
    void differentFieldCount() {
      Schema twoFields =
          new Schema(
              List.of(
                  new Field("a", FieldType.nullable(new ArrowType.Int(32, true)), null),
                  new Field("b", FieldType.nullable(new ArrowType.Int(32, true)), null)));
      assertFalse(BatchConsolidator.schemasAreCompatible(INT_SCHEMA, twoFields));
    }

    @Test
    void differentFieldTypes() {
      assertFalse(BatchConsolidator.schemasAreCompatible(INT_SCHEMA, STRING_SCHEMA));
    }

    @Test
    void differentNullability() {
      Schema nonNullable =
          new Schema(
              List.of(
                  new Field("id", new FieldType(false, new ArrowType.Int(32, true), null), null)));
      assertFalse(BatchConsolidator.schemasAreCompatible(INT_SCHEMA, nonNullable));
    }

    @Test
    void differentFieldNames() {
      Schema keySchema =
          new Schema(
              List.of(new Field("key", FieldType.nullable(new ArrowType.Int(32, true)), null)));
      assertFalse(BatchConsolidator.schemasAreCompatible(INT_SCHEMA, keySchema));
    }

    @Test
    void metadataIgnored() {
      Schema withMeta =
          new Schema(
              List.of(
                  new Field(
                      "id",
                      new FieldType(true, new ArrowType.Int(32, true), null, Map.of("k", "v")),
                      null)));
      assertTrue(BatchConsolidator.schemasAreCompatible(INT_SCHEMA, withMeta));
    }

    @Test
    void sameTypesDifferentBitWidth() {
      assertFalse(BatchConsolidator.schemasAreCompatible(INT_SCHEMA, BIGINT_SCHEMA));
    }
  }

  @Nested
  @DisplayName("Memory management")
  class MemoryManagement {

    @Test
    void consumedBatchesClosed() {
      VectorSchemaRoot a = createIntBatch(1, 2);
      VectorSchemaRoot b = createIntBatch(3, 4);
      VectorSchemaRoot c = createIntBatch(5, 6);
      long memBefore = allocator.getAllocatedMemory();
      assertTrue(memBefore > 0);

      List<VectorSchemaRoot> result = BatchConsolidator.consolidate(List.of(a, b, c));
      assertEquals(1, result.size());
      // Only the consolidated result should hold memory
      // (original batches b, c should be closed)

      closeAll(result);
      assertEquals(0, allocator.getAllocatedMemory());
    }

    @Test
    void allocatorCleanAfterConsolidation() {
      VectorSchemaRoot a = createIntBatch(1, 2, 3);
      VectorSchemaRoot b = createIntBatch(4, 5, 6);
      List<VectorSchemaRoot> result = BatchConsolidator.consolidate(List.of(a, b));
      closeAll(result);
      assertEquals(0, allocator.getAllocatedMemory());
    }

    @Test
    void allocatorCleanAfterMixedSchemaConsolidation() {
      VectorSchemaRoot a1 = createIntBatch(1, 2);
      VectorSchemaRoot a2 = createIntBatch(3, 4);
      VectorSchemaRoot b = createStringBatch("x");
      VectorSchemaRoot a3 = createIntBatch(5, 6);
      List<VectorSchemaRoot> result = BatchConsolidator.consolidate(List.of(a1, a2, b, a3));
      closeAll(result);
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }
}
