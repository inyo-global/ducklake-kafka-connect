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

import com.inyo.ducklake.ingestor.DucklakeWriter;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.VectorBatchAppender;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Connect sink task that writes records to DuckLake.
 *
 * <p>This implementation embraces Kafka Connect's single-threaded task model:
 *
 * <ul>
 *   <li>put() is called by a single worker thread - no locking needed
 *   <li>preCommit() handles time-based flushing (called at offset.flush.interval.ms)
 *   <li>Size/count thresholds are checked inline in put() for immediate flushes
 * </ul>
 *
 * <p>This design eliminates the need for background schedulers, thread pools, and locking
 * primitives that were previously used for time-based flushing.
 */
public class DucklakeSinkTask extends SinkTask {
  private static final Logger LOG = LoggerFactory.getLogger(DucklakeSinkTask.class);
  private DucklakeSinkConfig config;
  private DucklakeConnectionFactory connectionFactory;
  private DucklakeWriterFactory writerFactory;
  private Map<TopicPartition, DucklakeWriter> writers;
  private BufferAllocator allocator;
  private SinkRecordToArrowConverter converter;

  // Buffering state - single-threaded access, no synchronization needed
  private Map<TopicPartition, PartitionBuffer> buffers;
  private int flushSize;
  private long fileSizeBytes;

  // Errant record reporter for sending bad records to DLQ
  private ErrantRecordReporter errantRecordReporter;

  /** Per-partition buffer to accumulate records before writing */
  private static class PartitionBuffer {
    final List<VectorSchemaRoot> pendingBatches = new ArrayList<>();
    long recordCount = 0;
    long estimatedBytes = 0;

    void add(VectorSchemaRoot root) {
      pendingBatches.add(root);
      recordCount += root.getRowCount();
      // Estimate bytes from Arrow buffer sizes
      estimatedBytes += root.getFieldVectors().stream().mapToLong(v -> v.getBufferSize()).sum();
    }

    void reset() {
      pendingBatches.clear();
      recordCount = 0;
      estimatedBytes = 0;
    }

    void close() {
      for (VectorSchemaRoot root : pendingBatches) {
        try {
          root.close();
        } catch (Exception e) {
          // Ignore close errors during cleanup
        }
      }
      reset();
    }

    boolean isEmpty() {
      return pendingBatches.isEmpty();
    }

    /** Check if size/count thresholds are exceeded (time-based flush handled by preCommit) */
    boolean shouldFlush(int flushSize, long fileSizeBytes) {
      if (pendingBatches.isEmpty()) {
        return false;
      }
      return recordCount >= flushSize || estimatedBytes >= fileSizeBytes;
    }
  }

  @Override
  public String version() {
    return DucklakeSinkConfig.VERSION;
  }

  @Override
  public void start(Map<String, String> map) {
    this.config = new DucklakeSinkConfig(DucklakeSinkConfig.CONFIG_DEF, map);
    this.connectionFactory = new DucklakeConnectionFactory(config);
    this.writers = new HashMap<>();
    this.buffers = new HashMap<>();
    this.allocator = new RootAllocator();
    this.converter = new SinkRecordToArrowConverter(allocator);

    // Initialize buffering configuration
    this.flushSize = config.getFlushSize();
    this.fileSizeBytes = config.getFileSizeBytes();

    LOG.info(
        "Buffering config: flushSize={}, fileSizeBytes={} "
            + "(time-based flush controlled by offset.flush.interval.ms)",
        flushSize,
        fileSizeBytes);

    // Initialize errant record reporter for DLQ support (may be null if not configured)
    try {
      this.errantRecordReporter = context.errantRecordReporter();
      if (errantRecordReporter != null) {
        LOG.info("Errant record reporter initialized - bad records will be sent to DLQ");
      } else {
        LOG.info("No errant record reporter available - bad records will cause task failure");
      }
    } catch (NoSuchMethodError | NoClassDefFoundError e) {
      // Older Kafka Connect versions may not have errantRecordReporter()
      LOG.warn("ErrantRecordReporter not available in this Kafka Connect version");
      this.errantRecordReporter = null;
    }
  }

  @Override
  @SuppressFBWarnings(
      value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR",
      justification = "Values are initialized in start() method")
  public void open(Collection<TopicPartition> partitions) {
    super.open(partitions);
    try {
      this.connectionFactory.create();
      this.writerFactory = new DucklakeWriterFactory(config, connectionFactory.getConnection());

      // Create one writer and buffer for each partition
      for (TopicPartition partition : partitions) {
        DucklakeWriter writer = writerFactory.create(partition.topic());
        writers.put(partition, writer);
        buffers.put(partition, new PartitionBuffer());
        LOG.info("Created writer and buffer for partition: {}", partition);
      }
    } catch (SQLException e) {
      throw new RuntimeException("Failed to create writers for partitions", e);
    }
  }

  @Override
  @SuppressFBWarnings(
      value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR",
      justification = "Values are initialized in start() method")
  public void put(Collection<SinkRecord> records) {
    if (records == null || records.isEmpty()) {
      return;
    }

    // Group records by partition
    Map<TopicPartition, List<SinkRecord>> recordsByPartition =
        SinkRecordToArrowConverter.groupRecordsByPartition(records);

    // Process each partition sequentially (single-threaded, no locking needed)
    for (Map.Entry<TopicPartition, List<SinkRecord>> entry : recordsByPartition.entrySet()) {
      processPartition(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Called by the framework before committing offsets (at offset.flush.interval.ms).
   *
   * <p>This is where we handle time-based flushing - the framework guarantees this is called
   * periodically, so we don't need our own background scheduler.
   */
  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    // Flush all non-empty buffers (time-based flush)
    for (Map.Entry<TopicPartition, PartitionBuffer> entry : buffers.entrySet()) {
      TopicPartition partition = entry.getKey();
      PartitionBuffer buffer = entry.getValue();

      if (!buffer.isEmpty()) {
        LOG.info(
            "Time-based flush for partition {} (preCommit): records={}, bytes={}",
            partition,
            buffer.recordCount,
            buffer.estimatedBytes);
        flushPartition(partition);
      }
    }

    return currentOffsets;
  }

  /** Process records for a single partition - buffer and flush if thresholds exceeded. */
  private void processPartition(TopicPartition partition, List<SinkRecord> partitionRecords) {
    try {
      // Detect if we have Arrow IPC data (VectorSchemaRoot) or traditional data
      boolean hasArrowIpcData =
          partitionRecords.stream().anyMatch(record -> record.value() instanceof VectorSchemaRoot);

      if (hasArrowIpcData) {
        bufferArrowIpcRecordsForPartition(partition, partitionRecords);
      } else {
        bufferTraditionalRecordsForPartition(partition, partitionRecords);
      }

      // Check if this partition needs flushing (size/count thresholds only)
      PartitionBuffer buffer = buffers.get(partition);
      if (buffer != null && buffer.shouldFlush(flushSize, fileSizeBytes)) {
        String reason = buffer.recordCount >= flushSize ? "record count" : "file size";
        LOG.info(
            "Flush triggered for partition {} (reason={}, records={}, bytes={})",
            partition,
            reason,
            buffer.recordCount,
            buffer.estimatedBytes);
        flushPartition(partition);
      }
    } catch (Exception e) {
      // Build a concise metadata sample for easier debugging (topic:partition@offset)
      var sb = new StringBuilder();
      sb.append("Error processing records for partition ")
          .append(partition)
          .append(". batchSize=")
          .append(partitionRecords.size())
          .append(". sampleOffsets=");
      var idx = 0;
      for (SinkRecord r : partitionRecords) {
        var offset = String.valueOf(r.kafkaOffset());
        sb.append("[")
            .append(r.topic())
            .append(":")
            .append(r.kafkaPartition())
            .append("@")
            .append(offset)
            .append("]");
        if (++idx >= 10) {
          sb.append("(truncated)");
          break;
        } else {
          sb.append(',');
        }
      }
      LOG.error(sb.toString(), e);
      throw new RuntimeException("Failed to process sink records", e);
    }
  }

  /** Flush all buffered data for a partition. */
  private void flushPartition(TopicPartition partition) {
    PartitionBuffer buffer = buffers.get(partition);
    if (buffer == null || buffer.isEmpty()) {
      return;
    }

    DucklakeWriter writer = writers.get(partition);
    if (writer == null) {
      LOG.warn(
          "No writer found for partition: {}, closing {} batches",
          partition,
          buffer.pendingBatches.size());
      buffer.close();
      return;
    }

    // Extract batches for writing
    List<VectorSchemaRoot> batches = new ArrayList<>(buffer.pendingBatches);
    long recordCount = buffer.recordCount;
    long estimatedBytes = buffer.estimatedBytes;

    // Reset buffer state before I/O (batches will be closed after write)
    buffer.reset();

    long actualAllocatedBytes = allocator.getAllocatedMemory();
    LOG.info(
        "Flushing partition {}: {} batches, {} records, estimatedBytes={}, allocatorBytes={}",
        partition,
        batches.size(),
        recordCount,
        estimatedBytes,
        actualAllocatedBytes);

    VectorSchemaRoot consolidatedRoot = null;
    try {
      consolidatedRoot = consolidateBatches(batches);
      if (consolidatedRoot != null && consolidatedRoot.getRowCount() > 0) {
        // Consolidated successfully - write single batch
        writer.write(consolidatedRoot);
      } else if (consolidatedRoot == null && !batches.isEmpty()) {
        // Consolidation failed (schema mismatch) - fall back to writing individually
        LOG.info(
            "Writing {} batches individually for partition {} due to schema differences",
            batches.size(),
            partition);
        for (VectorSchemaRoot root : batches) {
          if (root.getRowCount() > 0) {
            writer.write(root);
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to write buffered data for partition: {}", partition, e);
      throw new RuntimeException("Failed to flush buffered data", e);
    } finally {
      // Close the consolidated root
      if (consolidatedRoot != null) {
        try {
          consolidatedRoot.close();
        } catch (Exception closeEx) {
          LOG.warn("Failed to close consolidated VectorSchemaRoot: {}", closeEx.getMessage());
        }
      }
      // Close all original batch roots
      closeBatches(batches);
    }
  }

  /** Close a list of VectorSchemaRoot batches, logging any errors. */
  private void closeBatches(List<VectorSchemaRoot> batches) {
    for (VectorSchemaRoot root : batches) {
      try {
        root.close();
      } catch (Exception closeEx) {
        LOG.warn("Failed to close VectorSchemaRoot: {}", closeEx.getMessage());
      }
    }
  }

  /**
   * Checks if all batches have compatible schemas for consolidation. Schemas are compatible if they
   * have the same fields in the same order with the same types. This method ignores field and
   * schema metadata, which can differ between batches even when the actual data types are the same.
   */
  private boolean haveSameSchema(List<VectorSchemaRoot> batches) {
    if (batches.size() <= 1) {
      return true;
    }
    Schema firstSchema = batches.get(0).getSchema();
    for (int i = 1; i < batches.size(); i++) {
      Schema otherSchema = batches.get(i).getSchema();
      if (!schemasAreCompatible(firstSchema, otherSchema)) {
        LOG.debug(
            "Schema mismatch between batch 0 and batch {}: {} vs {}", i, firstSchema, otherSchema);
        return false;
      }
    }
    return true;
  }

  /**
   * Checks if two schemas are compatible for consolidation. Compatible means they have the same
   * number of fields, and each field has the same name, type, and nullability. Metadata differences
   * are ignored since they don't affect data compatibility.
   */
  private boolean schemasAreCompatible(Schema schema1, Schema schema2) {
    List<Field> fields1 = schema1.getFields();
    List<Field> fields2 = schema2.getFields();

    if (fields1.size() != fields2.size()) {
      return false;
    }

    for (int i = 0; i < fields1.size(); i++) {
      Field f1 = fields1.get(i);
      Field f2 = fields2.get(i);

      // Compare field name, type, and nullability (ignore metadata)
      if (!f1.getName().equals(f2.getName())) {
        return false;
      }
      if (!f1.getType().equals(f2.getType())) {
        return false;
      }
      if (f1.isNullable() != f2.isNullable()) {
        return false;
      }
      // For complex types with children, recursively check child fields
      if (!childFieldsAreCompatible(f1.getChildren(), f2.getChildren())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Recursively checks if child fields are compatible. This handles nested types like structs and
   * lists.
   */
  private boolean childFieldsAreCompatible(List<Field> children1, List<Field> children2) {
    if (children1.size() != children2.size()) {
      return false;
    }
    for (int i = 0; i < children1.size(); i++) {
      Field c1 = children1.get(i);
      Field c2 = children2.get(i);
      if (!c1.getName().equals(c2.getName())) {
        return false;
      }
      if (!c1.getType().equals(c2.getType())) {
        return false;
      }
      if (c1.isNullable() != c2.isNullable()) {
        return false;
      }
      if (!childFieldsAreCompatible(c1.getChildren(), c2.getChildren())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Consolidates multiple VectorSchemaRoot batches into a single VectorSchemaRoot. This reduces the
   * number of write operations to DuckLake, improving throughput. Returns null if batches have
   * incompatible schemas (caller should fall back to writing individually).
   */
  private VectorSchemaRoot consolidateBatches(List<VectorSchemaRoot> batches) {
    if (batches == null || batches.isEmpty()) {
      return null;
    }

    // Check if all batches have compatible schemas
    if (!haveSameSchema(batches)) {
      LOG.warn(
          "Cannot consolidate {} batches due to schema mismatch - will write individually",
          batches.size());
      return null;
    }

    if (batches.size() == 1) {
      // Single batch - no consolidation needed, but we need to copy it since
      // the original will be closed separately
      VectorSchemaRoot source = batches.get(0);
      VectorSchemaRoot copy = VectorSchemaRoot.create(source.getSchema(), allocator);
      try {
        copy.allocateNew();
        for (int i = 0; i < source.getFieldVectors().size(); i++) {
          FieldVector sourceVector = source.getFieldVectors().get(i);
          FieldVector targetVector = copy.getFieldVectors().get(i);
          for (int row = 0; row < source.getRowCount(); row++) {
            targetVector.copyFromSafe(row, row, sourceVector);
          }
        }
        copy.setRowCount(source.getRowCount());
        return copy;
      } catch (Exception e) {
        // Clean up on failure to prevent memory leak
        try {
          copy.close();
        } catch (Exception closeEx) {
          LOG.warn("Failed to close copy root during cleanup: {}", closeEx.getMessage());
        }
        throw e;
      }
    }

    // Use schema from first batch
    Schema schema = batches.get(0).getSchema();

    // Calculate total row count
    int totalRows = batches.stream().mapToInt(VectorSchemaRoot::getRowCount).sum();

    // Create consolidated root
    VectorSchemaRoot consolidated = VectorSchemaRoot.create(schema, allocator);
    try {
      consolidated.allocateNew();

      // Copy data from each batch using VectorBatchAppender
      VectorSchemaRoot[] batchArray = batches.toArray(new VectorSchemaRoot[0]);
      for (int i = 0; i < consolidated.getFieldVectors().size(); i++) {
        FieldVector targetVector = consolidated.getFieldVectors().get(i);
        FieldVector[] sourceVectors = new FieldVector[batchArray.length];
        for (int j = 0; j < batchArray.length; j++) {
          sourceVectors[j] = batchArray[j].getFieldVectors().get(i);
        }
        VectorBatchAppender.batchAppend(targetVector, sourceVectors);
      }

      consolidated.setRowCount(totalRows);
      LOG.debug(
          "Consolidated {} batches into single batch with {} rows", batches.size(), totalRows);
      return consolidated;
    } catch (Exception e) {
      // Clean up on failure to prevent memory leak
      try {
        consolidated.close();
      } catch (Exception closeEx) {
        LOG.warn("Failed to close consolidated root during cleanup: {}", closeEx.getMessage());
      }
      throw e;
    }
  }

  /** Buffer Arrow IPC records for a single partition */
  private void bufferArrowIpcRecordsForPartition(
      TopicPartition partition, List<SinkRecord> records) {
    LOG.debug("Buffering {} Arrow IPC records for partition {}", records.size(), partition);

    PartitionBuffer buffer = buffers.get(partition);
    if (buffer == null) {
      LOG.warn("No buffer found for partition: {}", partition);
      // Close VectorSchemaRoot objects if no buffer
      for (SinkRecord record : records) {
        if (record.value() instanceof VectorSchemaRoot root) {
          try {
            root.close();
          } catch (Exception e) {
            // Ignore
          }
        }
      }
      return;
    }

    for (SinkRecord record : records) {
      if (record.value() instanceof VectorSchemaRoot vectorSchemaRoot) {
        buffer.add(vectorSchemaRoot);
      } else {
        LOG.warn(
            "Mixed data types detected - record value is not VectorSchemaRoot: {}",
            record.value().getClass().getName());
      }
    }
  }

  /** Buffer traditional records for a single partition */
  private void bufferTraditionalRecordsForPartition(
      TopicPartition partition, List<SinkRecord> records) {
    LOG.debug("Buffering {} traditional records for partition {}", records.size(), partition);

    PartitionBuffer buffer = buffers.get(partition);
    if (buffer == null) {
      LOG.warn("No buffer found for partition: {}", partition);
      return;
    }

    // Convert records for this single partition
    Map<TopicPartition, List<SinkRecord>> singlePartitionMap = new HashMap<>();
    singlePartitionMap.put(partition, records);

    try {
      // Fast path: try batch conversion
      Map<TopicPartition, VectorSchemaRoot> vectorsByPartition =
          converter.convertRecordsByPartition(singlePartitionMap);

      VectorSchemaRoot vectorSchemaRoot = vectorsByPartition.get(partition);
      if (vectorSchemaRoot != null) {
        buffer.add(vectorSchemaRoot);
      }
    } catch (RuntimeException e) {
      // Check if this is a schema conflict error (e.g., string vs timestamp type mismatch)
      if (isSchemaConflictError(e)) {
        LOG.warn(
            "Schema conflict detected for partition {}, attempting per-record conversion to "
                + "identify bad records: {}",
            partition,
            e.getMessage());
        handleSchemaConflictWithDLQ(partition, records, buffer, e);
      } else {
        // Re-throw non-schema errors
        throw e;
      }
    }
  }

  /**
   * Checks if an exception is caused by a schema conflict (e.g., incompatible types like string vs
   * timestamp).
   */
  private boolean isSchemaConflictError(Throwable e) {
    // Check the exception chain for schema-related errors
    Throwable current = e;
    while (current != null) {
      String message = current.getMessage();
      if (message != null
          && (message.contains("Cannot unify incompatible types")
              || message.contains("Incompatible type for column")
              || message.contains("conflicting types"))) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  /**
   * Handles schema conflicts by converting records one-by-one to identify bad records and routing
   * them to DLQ.
   */
  private void handleSchemaConflictWithDLQ(
      TopicPartition partition,
      List<SinkRecord> records,
      PartitionBuffer buffer,
      RuntimeException originalError) {

    if (errantRecordReporter == null) {
      // No DLQ configured - fail the entire batch
      LOG.error(
          "Schema conflict in partition {} and no DLQ configured. "
              + "Configure errors.deadletterqueue.topic.name to route bad records to DLQ.",
          partition);
      throw originalError;
    }

    LOG.info(
        "Converting {} records individually to identify bad records for partition {}",
        records.size(),
        partition);

    List<SinkRecord> goodRecords = new ArrayList<>();
    int badRecordCount = 0;

    // Try converting each record individually
    for (SinkRecord record : records) {
      try {
        Map<TopicPartition, List<SinkRecord>> singleRecordMap = new HashMap<>();
        singleRecordMap.put(partition, Collections.singletonList(record));

        Map<TopicPartition, VectorSchemaRoot> result =
            converter.convertRecordsByPartition(singleRecordMap);

        VectorSchemaRoot root = result.get(partition);
        if (root != null) {
          // Record converted successfully - close it for now, we'll re-batch good records
          root.close();
          goodRecords.add(record);
        }
      } catch (RuntimeException recordError) {
        // This record caused the error - send to DLQ
        badRecordCount++;
        LOG.warn(
            "Sending bad record to DLQ: topic={}, partition={}, offset={}, error={}",
            record.topic(),
            record.kafkaPartition(),
            record.kafkaOffset(),
            recordError.getMessage());

        try {
          errantRecordReporter.report(record, recordError);
        } catch (Exception dlqError) {
          LOG.error(
              "Failed to report record to DLQ: topic={}, partition={}, offset={}",
              record.topic(),
              record.kafkaPartition(),
              record.kafkaOffset(),
              dlqError);
          // If DLQ reporting fails, we need to fail the task
          throw new ConnectException("Failed to report bad record to DLQ", dlqError);
        }
      }
    }

    LOG.info(
        "Partition {}: {} records sent to DLQ, {} good records remaining",
        partition,
        badRecordCount,
        goodRecords.size());

    // Convert and buffer the good records
    if (!goodRecords.isEmpty()) {
      Map<TopicPartition, List<SinkRecord>> goodRecordsMap = new HashMap<>();
      goodRecordsMap.put(partition, goodRecords);

      try {
        Map<TopicPartition, VectorSchemaRoot> vectorsByPartition =
            converter.convertRecordsByPartition(goodRecordsMap);

        VectorSchemaRoot vectorSchemaRoot = vectorsByPartition.get(partition);
        if (vectorSchemaRoot != null) {
          buffer.add(vectorSchemaRoot);
        }
      } catch (RuntimeException e) {
        // This shouldn't happen if individual conversions succeeded, but handle it
        LOG.error("Unexpected error converting good records after DLQ filtering", e);
        throw e;
      }
    }
  }

  @Override
  public void stop() {
    LOG.info("Stopping DucklakeSinkTask, flushing remaining buffers...");

    // Flush all remaining buffered data
    for (TopicPartition partition : buffers.keySet()) {
      try {
        flushPartition(partition);
      } catch (Exception e) {
        LOG.warn("Failed to flush partition {} during stop: {}", partition, e.getMessage());
      }
    }

    // Clean up buffers
    for (PartitionBuffer buffer : buffers.values()) {
      buffer.close();
    }
    buffers.clear();

    try {
      if (writers != null) {
        for (DucklakeWriter w : writers.values()) {
          try {
            w.close();
          } catch (Exception e) {
            LOG.warn("Failed closing writer: {}", e.getMessage());
          }
        }
        writers.clear();
        LOG.info("Cleared all writers");
      }
      if (converter != null) {
        try {
          converter.close();
        } catch (Exception e) {
          LOG.warn("Failed closing converter: {}", e.getMessage());
        }
      }
      if (allocator != null) {
        allocator.close();
      }
      if (connectionFactory != null) {
        connectionFactory.close();
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to stop DucklakeSinkTask", e);
    }
  }
}
