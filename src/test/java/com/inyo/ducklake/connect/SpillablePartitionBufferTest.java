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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SpillablePartitionBufferTest {

  @TempDir Path tempDir;

  private BufferAllocator allocator;
  private SpillablePartitionBuffer buffer;

  @BeforeEach
  void setUp() {
    allocator = new RootAllocator();
    buffer = new SpillablePartitionBuffer(tempDir);
  }

  @AfterEach
  void tearDown() {
    if (buffer != null) {
      buffer.clear();
    }
    if (allocator != null) {
      allocator.close();
    }
  }

  @Test
  void testEmptyBuffer() {
    assertTrue(buffer.isEmpty());
    assertEquals(0, buffer.getBatchCount());
    assertEquals(0, buffer.getRecordCount());
    assertEquals(0, buffer.getEstimatedBytes());
  }

  @Test
  void testAddAndReadSingleBatch() {
    // Create a simple batch
    VectorSchemaRoot batch = createTestBatch(10);

    // Add to buffer (this spills to disk)
    buffer.add(batch);

    // Verify buffer state
    assertFalse(buffer.isEmpty());
    assertEquals(1, buffer.getBatchCount());
    assertEquals(10, buffer.getRecordCount());
    assertTrue(buffer.getEstimatedBytes() > 0);

    // Verify file was created
    assertTrue(spillFileExists());

    // Read back
    List<VectorSchemaRoot> readBatches = buffer.readBatches(allocator);
    assertEquals(1, readBatches.size());

    VectorSchemaRoot readBatch = readBatches.get(0);
    assertEquals(10, readBatch.getRowCount());

    // Verify data integrity
    IntVector idVector = (IntVector) readBatch.getVector("id");
    VarCharVector nameVector = (VarCharVector) readBatch.getVector("name");

    for (int i = 0; i < 10; i++) {
      assertEquals(i, idVector.get(i));
      assertEquals("name-" + i, new String(nameVector.get(i), StandardCharsets.UTF_8));
    }

    // Clean up
    readBatch.close();
  }

  @Test
  void testAddMultipleBatches() {
    // Add multiple batches
    buffer.add(createTestBatch(5));
    buffer.add(createTestBatch(10));
    buffer.add(createTestBatch(15));

    assertEquals(3, buffer.getBatchCount());
    assertEquals(30, buffer.getRecordCount());

    // Read back
    List<VectorSchemaRoot> readBatches = buffer.readBatches(allocator);
    assertEquals(3, readBatches.size());
    assertEquals(5, readBatches.get(0).getRowCount());
    assertEquals(10, readBatches.get(1).getRowCount());
    assertEquals(15, readBatches.get(2).getRowCount());

    // Clean up
    for (VectorSchemaRoot batch : readBatches) {
      batch.close();
    }
  }

  @Test
  void testClearDeletesFiles() {
    buffer.add(createTestBatch(10));
    assertTrue(spillFileExists());

    buffer.clear();

    assertTrue(buffer.isEmpty());
    assertEquals(0, buffer.getBatchCount());
    // Files should be deleted
    assertFalse(spillFileExists());
  }

  @Test
  void testShouldFlushByRecordCount() {
    buffer.add(createTestBatch(100));

    // Should not flush yet (threshold is 1000)
    assertFalse(buffer.shouldFlush(1000, Long.MAX_VALUE, Long.MAX_VALUE));

    // Add more to exceed threshold
    for (int i = 0; i < 10; i++) {
      buffer.add(createTestBatch(100));
    }

    // Now should flush (1100 >= 1000)
    assertTrue(buffer.shouldFlush(1000, Long.MAX_VALUE, Long.MAX_VALUE));
  }

  @Test
  void testShouldFlushByFileSize() {
    buffer.add(createTestBatch(1000));

    long estimatedBytes = buffer.getEstimatedBytes();

    // Should flush if threshold is less than estimated bytes
    assertTrue(buffer.shouldFlush(Integer.MAX_VALUE, estimatedBytes - 1, Long.MAX_VALUE));

    // Should not flush if threshold is greater
    assertFalse(buffer.shouldFlush(Integer.MAX_VALUE, estimatedBytes + 1000, Long.MAX_VALUE));
  }

  @Test
  void testShouldFlushByTimeInterval() throws InterruptedException {
    buffer.add(createTestBatch(10));

    // Should not flush immediately
    assertFalse(buffer.shouldFlush(Integer.MAX_VALUE, Long.MAX_VALUE, 100));

    // Wait for interval to pass
    Thread.sleep(150);

    // Now should flush
    assertTrue(buffer.shouldFlush(Integer.MAX_VALUE, Long.MAX_VALUE, 100));
  }

  @Test
  void testAddNullBatch() {
    buffer.add(null);
    assertTrue(buffer.isEmpty());
  }

  @Test
  void testAddEmptyBatch() {
    VectorSchemaRoot emptyBatch = createTestBatch(0);
    buffer.add(emptyBatch);
    assertTrue(buffer.isEmpty());
  }

  @Test
  void testMemoryReleasedAfterSpill() {
    long memoryBefore = allocator.getAllocatedMemory();

    // Create and spill a batch
    VectorSchemaRoot batch = createTestBatch(1000);
    long memoryWithBatch = allocator.getAllocatedMemory();
    assertTrue(memoryWithBatch > memoryBefore);

    buffer.add(batch); // This should close the batch

    // Memory should be released (batch was closed after spilling)
    long memoryAfterSpill = allocator.getAllocatedMemory();
    assertEquals(memoryBefore, memoryAfterSpill);
  }

  private VectorSchemaRoot createTestBatch(int rowCount) {
    Schema schema =
        new Schema(
            List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));

    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();

    IntVector idVector = (IntVector) root.getVector("id");
    VarCharVector nameVector = (VarCharVector) root.getVector("name");

    for (int i = 0; i < rowCount; i++) {
      idVector.setSafe(i, i);
      nameVector.setSafe(i, ("name-" + i).getBytes(StandardCharsets.UTF_8));
    }

    root.setRowCount(rowCount);
    return root;
  }

  private boolean spillFileExists() {
    try {
      return Files.walk(tempDir).anyMatch(p -> p.toString().endsWith(".arrow"));
    } catch (IOException e) {
      return false;
    }
  }
}
