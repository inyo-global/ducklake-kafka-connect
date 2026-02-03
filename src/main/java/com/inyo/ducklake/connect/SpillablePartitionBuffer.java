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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A partition buffer that spills Arrow batches to disk instead of holding them in memory. This
 * dramatically reduces memory pressure for large buffer configurations by only keeping file paths
 * in memory, not the actual Arrow data.
 *
 * <p>Each batch is written to a separate Arrow IPC file on disk. When flush time comes, the batches
 * are read back from disk, processed, and the files are deleted.
 */
public class SpillablePartitionBuffer {

  private static final Logger LOG = LoggerFactory.getLogger(SpillablePartitionBuffer.class);

  private final Path spillDirectory;
  private final List<SpilledBatch> spilledBatches = new ArrayList<>();
  private long recordCount = 0;
  private long estimatedBytes = 0;
  private long lastFlushTime = System.currentTimeMillis();

  /** Metadata about a spilled batch file. */
  private static class SpilledBatch {
    final Path filePath;
    final int rowCount;
    final long byteSize;
    final Schema schema;

    SpilledBatch(Path filePath, int rowCount, long byteSize, Schema schema) {
      this.filePath = filePath;
      this.rowCount = rowCount;
      this.byteSize = byteSize;
      this.schema = schema;
    }
  }

  /**
   * Creates a new spillable partition buffer.
   *
   * @param spillDirectory the directory to write spill files to
   */
  @SuppressFBWarnings(
      value = "CT_CONSTRUCTOR_THROW",
      justification = "Failing fast on invalid spill directory is intentional")
  public SpillablePartitionBuffer(Path spillDirectory) {
    this.spillDirectory = spillDirectory;
    try {
      Files.createDirectories(spillDirectory);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create spill directory: " + spillDirectory, e);
    }
  }

  /**
   * Adds a VectorSchemaRoot batch to the buffer by spilling it to disk. The batch is immediately
   * closed after writing to free memory.
   *
   * @param root the Arrow batch to spill
   */
  public void add(VectorSchemaRoot root) {
    if (root == null || root.getRowCount() == 0) {
      if (root != null) {
        root.close();
      }
      return;
    }

    Path spillFile = spillDirectory.resolve("batch-" + UUID.randomUUID() + ".arrow");
    long byteSize = root.getFieldVectors().stream().mapToLong(FieldVector::getBufferSize).sum();

    try {
      writeArrowFile(root, spillFile);

      spilledBatches.add(
          new SpilledBatch(spillFile, root.getRowCount(), byteSize, root.getSchema()));
      recordCount += root.getRowCount();
      estimatedBytes += byteSize;

      LOG.debug(
          "Spilled batch to {}: {} rows, {} bytes",
          spillFile.getFileName(),
          root.getRowCount(),
          byteSize);
    } catch (IOException e) {
      // Clean up partial file on failure
      try {
        Files.deleteIfExists(spillFile);
      } catch (IOException ignored) {
        // Ignore cleanup errors
      }
      throw new RuntimeException("Failed to spill batch to disk", e);
    } finally {
      // Always close the root to free memory - this is the whole point of spilling
      root.close();
    }
  }

  /**
   * Reads all spilled batches back from disk. Missing files are skipped with a warning - this can
   * happen after pod restarts or task rebalancing in Kubernetes where /tmp is ephemeral. Lost
   * buffered data is acceptable since Kafka will resend from the last committed offset.
   *
   * @param allocator the Arrow allocator to use for reading
   * @return list of VectorSchemaRoot batches successfully read from disk
   */
  public List<VectorSchemaRoot> readBatches(BufferAllocator allocator) {
    List<VectorSchemaRoot> batches = new ArrayList<>();
    int skippedCount = 0;

    for (SpilledBatch spilled : spilledBatches) {
      // Skip files that don't exist (e.g., after pod restart in K8s)
      if (!Files.exists(spilled.filePath)) {
        LOG.warn(
            "Spill file does not exist (likely lost during pod restart/rebalance): {}",
            spilled.filePath);
        skippedCount++;
        continue;
      }

      try {
        VectorSchemaRoot root = readArrowFile(spilled.filePath, allocator);
        if (root != null) {
          batches.add(root);
        }
      } catch (IOException e) {
        LOG.warn(
            "Failed to read spilled batch from {} (skipping): {}",
            spilled.filePath,
            e.getMessage());
        skippedCount++;
        // Continue reading other batches instead of failing completely
      }
    }

    if (skippedCount > 0) {
      LOG.warn(
          "Skipped {} spill files that could not be read. "
              + "This is expected after pod restart/rebalance - Kafka will resend the data.",
          skippedCount);
    }

    return batches;
  }

  /**
   * Clears the buffer by deleting all spill files and resetting counters. Call this after
   * successfully flushing the batches.
   */
  public void clear() {
    for (SpilledBatch spilled : spilledBatches) {
      try {
        Files.deleteIfExists(spilled.filePath);
        LOG.debug("Deleted spill file: {}", spilled.filePath.getFileName());
      } catch (IOException e) {
        LOG.warn("Failed to delete spill file: {}", spilled.filePath, e);
      }
    }
    spilledBatches.clear();
    recordCount = 0;
    estimatedBytes = 0;
    lastFlushTime = System.currentTimeMillis();
  }

  /**
   * Checks if the buffer should be flushed based on thresholds.
   *
   * @param flushSize record count threshold
   * @param fileSizeBytes byte size threshold
   * @param flushIntervalMs time interval threshold
   * @return true if any threshold is exceeded
   */
  public boolean shouldFlush(int flushSize, long fileSizeBytes, long flushIntervalMs) {
    if (spilledBatches.isEmpty()) {
      return false;
    }
    if (recordCount >= flushSize) {
      return true;
    }
    if (estimatedBytes >= fileSizeBytes) {
      return true;
    }
    if (System.currentTimeMillis() - lastFlushTime >= flushIntervalMs) {
      return true;
    }
    return false;
  }

  /** Returns the number of spilled batches. */
  public int getBatchCount() {
    return spilledBatches.size();
  }

  /** Returns the total record count across all batches. */
  public long getRecordCount() {
    return recordCount;
  }

  /** Returns the estimated total bytes across all batches. */
  public long getEstimatedBytes() {
    return estimatedBytes;
  }

  /** Returns the last flush time. */
  public long getLastFlushTime() {
    return lastFlushTime;
  }

  /** Returns whether the buffer is empty. */
  public boolean isEmpty() {
    return spilledBatches.isEmpty();
  }

  /** Writes a VectorSchemaRoot to an Arrow IPC file. */
  private void writeArrowFile(VectorSchemaRoot root, Path path) throws IOException {
    // Ensure parent directory exists - it might have been deleted by another task's stop()
    // during rebalancing (race condition between old task cleanup and new task writes)
    Path parent = path.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }

    try (var channel =
            Files.newByteChannel(
                path,
                java.nio.file.StandardOpenOption.CREATE,
                java.nio.file.StandardOpenOption.WRITE,
                java.nio.file.StandardOpenOption.TRUNCATE_EXISTING);
        var writer = new ArrowFileWriter(root, null, channel)) {
      writer.start();
      writer.writeBatch();
      writer.end();
    }
  }

  /** Reads a VectorSchemaRoot from an Arrow IPC file. */
  private VectorSchemaRoot readArrowFile(Path path, BufferAllocator allocator) throws IOException {
    try (var channel = Files.newByteChannel(path, java.nio.file.StandardOpenOption.READ);
        var reader = new ArrowFileReader(channel, allocator)) {

      // Load the schema
      var schema = reader.getVectorSchemaRoot().getSchema();
      var result = VectorSchemaRoot.create(schema, allocator);
      result.allocateNew();

      // Read all record batches (typically just one per file)
      int totalRows = 0;
      while (reader.loadNextBatch()) {
        var source = reader.getVectorSchemaRoot();
        int sourceRows = source.getRowCount();

        // Copy data from source to result
        // copyFromSafe(fromIndex, thisIndex, from) - copy from sourceVector[row] to
        // targetVector[totalRows + row]
        for (int i = 0; i < source.getFieldVectors().size(); i++) {
          var sourceVector = source.getFieldVectors().get(i);
          var targetVector = result.getFieldVectors().get(i);
          for (int row = 0; row < sourceRows; row++) {
            targetVector.copyFromSafe(row, totalRows + row, sourceVector);
          }
        }
        totalRows += sourceRows;
      }

      result.setRowCount(totalRows);
      return result;
    }
  }
}
