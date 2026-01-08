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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.connect.sink.SinkTaskContext;

public class DucklakeSinkTask extends SinkTask {
  private static final Logger LOG = LoggerFactory.getLogger(DucklakeSinkTask.class);
  private DucklakeSinkConfig config;
  private DucklakeConnectionFactory connectionFactory;
  private DucklakeWriterFactory writerFactory;
  private Map<TopicPartition, DucklakeWriter> writers;
  private BufferAllocator allocator;
  private SinkRecordToArrowConverter converter;
  private DucklakeMetrics metrics;
  private SinkTaskContext context;

  // Buffering state
  private Map<TopicPartition, PartitionBuffer> buffers;
  private int flushSize;
  private long flushIntervalMs;
  private long fileSizeBytes;
  private ScheduledExecutorService flushScheduler;

  // Per-partition locks to allow parallel processing across partitions
  private final ConcurrentHashMap<TopicPartition, ReentrantLock> partitionLocks =
      new ConcurrentHashMap<>();

  // Track skipped time-based flushes per partition
  private final ConcurrentHashMap<TopicPartition, AtomicInteger> consecutiveFlushSkips =
      new ConcurrentHashMap<>();
  private static final int MAX_CONSECUTIVE_SKIPS_BEFORE_WARNING = 5;
  private static final long FLUSH_LOCK_TIMEOUT_MS = 100;

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
  public void initialize(SinkTaskContext context) {
    super.initialize(context);
    this.context = context;
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

    LOG.info(
        "Buffering config: flushSize={}, flushIntervalMs={}, fileSizeBytes={}",
        flushSize,
        flushIntervalMs,
        fileSizeBytes);

    // Start scheduled flush checker (runs every 1 second to check time-based flush)
    this.flushScheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "ducklake-flush-scheduler");
              t.setDaemon(true);
              return t;
            });
    flushScheduler.scheduleAtFixedRate(this::checkTimeBasedFlush, 1, 1, TimeUnit.SECONDS);
  }

  /** Periodic check for time-based flush - uses per-partition locking */
  private void checkTimeBasedFlush() {
    long now = System.currentTimeMillis();

    for (Map.Entry<TopicPartition, PartitionBuffer> entry : buffers.entrySet()) {
      TopicPartition partition = entry.getKey();
      PartitionBuffer buffer = entry.getValue();

      // Skip if buffer is empty or not due for flush
      if (buffer.pendingBatches.isEmpty() || (now - buffer.lastFlushTime) < flushIntervalMs) {
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
        if (!buffer.pendingBatches.isEmpty() && (now - buffer.lastFlushTime) >= flushIntervalMs) {
          LOG.info(
              "Time-based flush triggered for partition {} (age={}ms, records={}, bytes={})",
              partition,
              now - buffer.lastFlushTime,
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
    // Initialize metrics - use Kafka's metrics from context or create new instance
    var metricsRegistry = getMetricsRegistry();
    var connectorName = map.getOrDefault("name", "ducklake-sink");
    var taskId = map.getOrDefault("task.id", "0");
    this.metrics = new DucklakeMetrics(metricsRegistry, connectorName, taskId);

    LOG.log(
        System.Logger.Level.INFO,
        "Started DucklakeSinkTask with metrics enabled for connector={0}, task={1}",
        connectorName,
        taskId);
  }

  /**
   * Gets the metrics registry from the context, or creates a new one if not available.
   *
   * @return the Metrics instance
   */
  private Metrics getMetricsRegistry() {
    // In Kafka Connect, the metrics registry is typically available through the context
    // However, the API doesn't expose it directly, so we create our own instance
    // that will still be registered with JMX automatically
    return new Metrics();
  }

  @Override
  @SuppressFBWarnings(
      value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR",
      justification = "Values are initialized in start() method")
  public void open(Collection<TopicPartition> partitions) {
    super.open(partitions);
    try {
      this.connectionFactory.create();
      this.writerFactory =
          new DucklakeWriterFactory(config, connectionFactory.getConnection(), metrics);

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
    try {
      // Record batch size metric
      metrics.recordBatchProcessed(records.size());

      // Detect if we have Arrow IPC data (VectorSchemaRoot) or traditional data
      boolean hasArrowIpcData =
          records.stream().anyMatch(record -> record.value() instanceof VectorSchemaRoot);

    // Process each partition with its own lock
    for (Map.Entry<TopicPartition, List<SinkRecord>> entry : recordsByPartition.entrySet()) {
      TopicPartition partition = entry.getKey();
      List<SinkRecord> partitionRecords = entry.getValue();

      ReentrantLock lock = partitionLocks.computeIfAbsent(partition, k -> new ReentrantLock());
      lock.lock();
      try {
        // Detect if we have Arrow IPC data (VectorSchemaRoot) or traditional data
        boolean hasArrowIpcData =
            partitionRecords.stream()
                .anyMatch(record -> record.value() instanceof VectorSchemaRoot);

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

    // Write each buffered batch
    for (VectorSchemaRoot root : buffer.pendingBatches) {
      try {
        writer.write(root);
      } catch (Exception e) {
        LOG.error("Failed to write buffered data for partition: {}", partition, e);
        // Clear buffer to avoid memory leaks even on failure
        buffer.clear();
        throw new RuntimeException("Failed to flush buffered data", e);
      } finally {
        try {
          root.close();
        } catch (Exception closeEx) {
          LOG.warn("Failed to close VectorSchemaRoot: {}", closeEx.getMessage());
        }
      }
    }

    // Clear the buffer (VectorSchemaRoots already closed in loop above)
    buffer.pendingBatches.clear();
    buffer.recordCount = 0;
    buffer.estimatedBytes = 0;
    buffer.lastFlushTime = System.currentTimeMillis();
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

    Map<TopicPartition, VectorSchemaRoot> vectorsByPartition =
        converter.convertRecordsByPartition(singlePartitionMap);

    VectorSchemaRoot vectorSchemaRoot = vectorsByPartition.get(partition);
    if (vectorSchemaRoot != null) {
      buffer.add(vectorSchemaRoot);
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
      if (metrics != null) {
        try {
          metrics.close();
        } catch (Exception e) {
          LOG.log(System.Logger.Level.WARNING, "Failed closing metrics: {0}", e.getMessage());
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
