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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.VectorBatchAppender;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DucklakeSinkTask extends SinkTask {
  private static final Logger LOG = LoggerFactory.getLogger(DucklakeSinkTask.class);
  private DucklakeSinkConfig config;
  private DucklakeConnectionFactory connectionFactory;
  private DucklakeWriterFactory writerFactory;
  private Map<TopicPartition, DucklakeWriter> writers;
  private BufferAllocator allocator;
  private SinkRecordToArrowConverter converter;

  // Buffering state
  private Map<TopicPartition, PartitionBuffer> buffers;
  private int flushSize;
  private long flushIntervalMs;
  private long fileSizeBytes;
  private ScheduledExecutorService flushScheduler;
  private ExecutorService partitionExecutor;
  private boolean parallelPartitionFlush;

  // Per-partition locks to allow parallel processing across partitions
  private final ConcurrentHashMap<TopicPartition, ReentrantLock> partitionLocks =
      new ConcurrentHashMap<>();

  // Track skipped time-based flushes per partition
  private final ConcurrentHashMap<TopicPartition, AtomicInteger> consecutiveFlushSkips =
      new ConcurrentHashMap<>();
  private static final int MAX_CONSECUTIVE_SKIPS_BEFORE_WARNING = 5;
  private static final long FLUSH_LOCK_TIMEOUT_MS = 100;

  // Errant record reporter for sending bad records to DLQ
  private ErrantRecordReporter errantRecordReporter;

  // Jitter configuration to stagger flushes and avoid PG row-level contention
  // Each partition gets a random jitter offset (0 to maxJitterMs) applied to its flush timing
  private static final double JITTER_FACTOR = 0.25; // 25% of flush interval as max jitter
  private final ConcurrentHashMap<TopicPartition, Long> partitionJitterMs =
      new ConcurrentHashMap<>();

  /** Per-partition buffer to accumulate records before writing */
  private static class PartitionBuffer {
    final List<VectorSchemaRoot> pendingBatches = new ArrayList<>();
    long recordCount = 0;
    long estimatedBytes = 0;
    long lastFlushTime = System.currentTimeMillis();

    void add(VectorSchemaRoot root) {
      pendingBatches.add(root);
      recordCount += root.getRowCount();
      // Estimate bytes from Arrow buffer sizes
      estimatedBytes += root.getFieldVectors().stream().mapToLong(v -> v.getBufferSize()).sum();
    }

    void clear() {
      // Close all VectorSchemaRoot to free memory
      for (VectorSchemaRoot root : pendingBatches) {
        try {
          root.close();
        } catch (Exception e) {
          // Ignore close errors during clear
        }
      }
      pendingBatches.clear();
      recordCount = 0;
      estimatedBytes = 0;
      lastFlushTime = System.currentTimeMillis();
    }

    boolean shouldFlush(int flushSize, long fileSizeBytes, long flushIntervalMs) {
      if (pendingBatches.isEmpty()) {
        return false;
      }
      // Flush if any threshold is exceeded
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
    this.flushIntervalMs = config.getFlushIntervalMs();
    this.fileSizeBytes = config.getFileSizeBytes();
    this.parallelPartitionFlush = config.isParallelPartitionFlushEnabled();

    int threadCount = config.getDuckDbThreads();
    LOG.info(
        "Buffering config: flushSize={}, flushIntervalMs={}, fileSizeBytes={}, "
            + "parallelPartitionFlush={}, duckdbThreads={}",
        flushSize,
        flushIntervalMs,
        fileSizeBytes,
        parallelPartitionFlush,
        threadCount);

    // Create executor for parallel partition processing
    if (parallelPartitionFlush) {
      this.partitionExecutor =
          Executors.newFixedThreadPool(
              Math.max(4, threadCount),
              r -> {
                Thread t = new Thread(r, "ducklake-partition-worker");
                t.setDaemon(true);
                return t;
              });
    }

    // Start scheduled flush checker (runs every 1 second to check time-based flush)
    this.flushScheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "ducklake-flush-scheduler");
              t.setDaemon(true);
              return t;
            });
    flushScheduler.scheduleAtFixedRate(this::checkTimeBasedFlush, 1, 1, TimeUnit.SECONDS);

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

  /**
   * Gets or assigns a random jitter offset for a partition. The jitter is assigned once per
   * partition and remains stable to ensure consistent flush timing. This staggering prevents
   * multiple partitions from flushing simultaneously, reducing PostgreSQL row-level contention.
   */
  private long getPartitionJitter(TopicPartition partition) {
    return partitionJitterMs.computeIfAbsent(
        partition,
        p -> {
          long maxJitter = (long) (flushIntervalMs * JITTER_FACTOR);
          long jitter = ThreadLocalRandom.current().nextLong(maxJitter + 1);
          LOG.debug(
              "Assigned flush jitter {}ms to partition {} (maxJitter={}ms)",
              jitter,
              partition,
              maxJitter);
          return jitter;
        });
  }

  /** Periodic check for time-based flush - uses per-partition locking with jitter */
  private void checkTimeBasedFlush() {
    long now = System.currentTimeMillis();

    for (Map.Entry<TopicPartition, PartitionBuffer> entry : buffers.entrySet()) {
      TopicPartition partition = entry.getKey();
      PartitionBuffer buffer = entry.getValue();

      // Get or assign jitter for this partition (stagger commits to avoid PG contention)
      long jitter = getPartitionJitter(partition);
      long effectiveFlushInterval = flushIntervalMs + jitter;

      // Skip if buffer is empty or not due for flush (accounting for jitter)
      if (buffer.pendingBatches.isEmpty()
          || (now - buffer.lastFlushTime) < effectiveFlushInterval) {
        continue;
      }

      // Try to acquire per-partition lock
      ReentrantLock lock = partitionLocks.computeIfAbsent(partition, k -> new ReentrantLock());
      boolean lockAcquired = false;
      try {
        lockAcquired = lock.tryLock(FLUSH_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }

      if (!lockAcquired) {
        AtomicInteger skips =
            consecutiveFlushSkips.computeIfAbsent(partition, k -> new AtomicInteger(0));
        int skipCount = skips.incrementAndGet();
        if (skipCount >= MAX_CONSECUTIVE_SKIPS_BEFORE_WARNING) {
          LOG.warn(
              "Flush check for partition {} skipped {} times - possible lock contention",
              partition,
              skipCount);
        }
        continue;
      }

      try {
        // Reset skip counter on successful lock acquisition
        consecutiveFlushSkips.computeIfAbsent(partition, k -> new AtomicInteger(0)).set(0);

        // Re-check condition under lock (double-check pattern)
        if (!buffer.pendingBatches.isEmpty()
            && (now - buffer.lastFlushTime) >= effectiveFlushInterval) {
          LOG.info(
              "Time-based flush triggered for partition {} (age={}ms, jitter={}ms, records={},"
                  + " bytes={})",
              partition,
              now - buffer.lastFlushTime,
              jitter,
              buffer.recordCount,
              buffer.estimatedBytes);
          flushPartition(partition);
        }
      } catch (Exception e) {
        LOG.warn("Error during time-based flush for partition {}: {}", partition, e.getMessage());
      } finally {
        lock.unlock();
      }
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

    // Group records by partition first (no lock needed)
    Map<TopicPartition, List<SinkRecord>> recordsByPartition =
        SinkRecordToArrowConverter.groupRecordsByPartition(records);

    if (parallelPartitionFlush && recordsByPartition.size() > 1) {
      // Process partitions in parallel for better throughput
      processPartitionsInParallel(recordsByPartition);
    } else {
      // Sequential processing for single partition or when parallel is disabled
      for (Map.Entry<TopicPartition, List<SinkRecord>> entry : recordsByPartition.entrySet()) {
        processPartition(entry.getKey(), entry.getValue());
      }
    }
  }

  /** Process partitions in parallel using the partition executor. */
  private void processPartitionsInParallel(
      Map<TopicPartition, List<SinkRecord>> recordsByPartition) {
    List<CompletableFuture<Void>> futures = new ArrayList<>(recordsByPartition.size());

    for (Map.Entry<TopicPartition, List<SinkRecord>> entry : recordsByPartition.entrySet()) {
      TopicPartition partition = entry.getKey();
      List<SinkRecord> partitionRecords = entry.getValue();

      CompletableFuture<Void> future =
          CompletableFuture.runAsync(
              () -> processPartition(partition, partitionRecords), partitionExecutor);
      futures.add(future);
    }

    // Wait for all partitions to complete and collect any errors
    try {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    } catch (Exception e) {
      LOG.error("Error during parallel partition processing", e);
      throw new RuntimeException("Failed to process partitions in parallel", e);
    }
  }

  /** Process a single partition with proper locking. */
  private void processPartition(TopicPartition partition, List<SinkRecord> partitionRecords) {
    ReentrantLock lock = partitionLocks.computeIfAbsent(partition, k -> new ReentrantLock());
    lock.lock();
    try {
      // Detect if we have Arrow IPC data (VectorSchemaRoot) or traditional data
      boolean hasArrowIpcData =
          partitionRecords.stream().anyMatch(record -> record.value() instanceof VectorSchemaRoot);

      if (hasArrowIpcData) {
        bufferArrowIpcRecordsForPartition(partition, partitionRecords);
      } else {
        bufferTraditionalRecordsForPartition(partition, partitionRecords);
      }

      // Check if this partition needs flushing
      checkAndFlushPartition(partition);
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
    } finally {
      lock.unlock();
    }
  }

  /** Check a single partition and flush if thresholds exceeded (called under partition lock) */
  private void checkAndFlushPartition(TopicPartition partition) {
    PartitionBuffer buffer = buffers.get(partition);
    if (buffer == null) {
      return;
    }

    // Check for global memory pressure from Arrow allocator
    long allocatedMemory = allocator.getAllocatedMemory();
    boolean memoryPressure = allocatedMemory > fileSizeBytes * buffers.size();

    if (memoryPressure && !buffer.pendingBatches.isEmpty()) {
      LOG.warn(
          "Memory pressure detected for partition {}: allocatorBytes={}, threshold={}",
          partition,
          allocatedMemory,
          fileSizeBytes * buffers.size());
    }

    // Flush if normal thresholds exceeded OR if under memory pressure with data buffered
    boolean shouldFlush =
        buffer.shouldFlush(flushSize, fileSizeBytes, flushIntervalMs)
            || (memoryPressure && !buffer.pendingBatches.isEmpty());

    if (shouldFlush) {
      String reason;
      if (memoryPressure && !buffer.shouldFlush(flushSize, fileSizeBytes, flushIntervalMs)) {
        reason = "memory pressure";
      } else if (buffer.recordCount >= flushSize) {
        reason = "record count";
      } else if (buffer.estimatedBytes >= fileSizeBytes) {
        reason = "file size";
      } else {
        reason = "time interval";
      }
      LOG.info(
          "Flush triggered for partition {} (reason={}, records={}, bytes={})",
          partition,
          reason,
          buffer.recordCount,
          buffer.estimatedBytes);
      flushPartition(partition);
    }
  }

  /** Flush all buffered data for a partition */
  private void flushPartition(TopicPartition partition) {
    PartitionBuffer buffer = buffers.get(partition);
    if (buffer == null || buffer.pendingBatches.isEmpty()) {
      return;
    }

    DucklakeWriter writer = writers.get(partition);
    if (writer == null) {
      LOG.warn("No writer found for partition: {}", partition);
      buffer.clear();
      return;
    }

    long actualAllocatedBytes = allocator.getAllocatedMemory();
    LOG.info(
        "Flushing partition {}: {} batches, {} records, estimatedBytes={}, allocatorBytes={}",
        partition,
        buffer.pendingBatches.size(),
        buffer.recordCount,
        buffer.estimatedBytes,
        actualAllocatedBytes);

    // Consolidate all batches into a single VectorSchemaRoot for one write operation
    VectorSchemaRoot consolidatedRoot = null;
    try {
      consolidatedRoot = consolidateBatches(buffer.pendingBatches);
      if (consolidatedRoot != null && consolidatedRoot.getRowCount() > 0) {
        writer.write(consolidatedRoot);
      }
    } catch (Exception e) {
      LOG.error("Failed to write buffered data for partition: {}", partition, e);
      // Clear buffer to avoid memory leaks even on failure
      buffer.clear();
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
      for (VectorSchemaRoot root : buffer.pendingBatches) {
        try {
          root.close();
        } catch (Exception closeEx) {
          LOG.warn("Failed to close VectorSchemaRoot: {}", closeEx.getMessage());
        }
      }
    }

    // Clear the buffer
    buffer.pendingBatches.clear();
    buffer.recordCount = 0;
    buffer.estimatedBytes = 0;
    buffer.lastFlushTime = System.currentTimeMillis();
  }

  /**
   * Consolidates multiple VectorSchemaRoot batches into a single VectorSchemaRoot. This reduces the
   * number of write operations to DuckLake, improving throughput.
   */
  private VectorSchemaRoot consolidateBatches(List<VectorSchemaRoot> batches) {
    if (batches == null || batches.isEmpty()) {
      return null;
    }
    if (batches.size() == 1) {
      // Single batch - no consolidation needed, but we need to copy it since
      // the original will be closed separately
      VectorSchemaRoot source = batches.get(0);
      VectorSchemaRoot copy = VectorSchemaRoot.create(source.getSchema(), allocator);
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
    }

    // Use schema from first batch
    Schema schema = batches.get(0).getSchema();

    // Calculate total row count
    int totalRows = batches.stream().mapToInt(VectorSchemaRoot::getRowCount).sum();

    // Create consolidated root
    VectorSchemaRoot consolidated = VectorSchemaRoot.create(schema, allocator);
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
    LOG.debug("Consolidated {} batches into single batch with {} rows", batches.size(), totalRows);
    return consolidated;
  }

  /** Buffer Arrow IPC records for a single partition (called under partition lock) */
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

  /** Buffer traditional records for a single partition (called under partition lock) */
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

    // Stop the flush scheduler
    if (flushScheduler != null) {
      flushScheduler.shutdown();
      try {
        if (!flushScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
          flushScheduler.shutdownNow();
        }
      } catch (InterruptedException e) {
        flushScheduler.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }

    // Stop the partition executor
    if (partitionExecutor != null) {
      partitionExecutor.shutdown();
      try {
        if (!partitionExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
          partitionExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        partitionExecutor.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }

    // Flush all remaining buffered data (acquire each partition lock)
    for (TopicPartition partition : buffers.keySet()) {
      ReentrantLock lock = partitionLocks.get(partition);
      if (lock != null) {
        lock.lock();
      }
      try {
        flushPartition(partition);
      } catch (Exception e) {
        LOG.warn("Failed to flush partition {} during stop: {}", partition, e.getMessage());
      } finally {
        if (lock != null) {
          lock.unlock();
        }
      }
    }
    buffers.clear();
    partitionLocks.clear();
    consecutiveFlushSkips.clear();
    partitionJitterMs.clear();

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
