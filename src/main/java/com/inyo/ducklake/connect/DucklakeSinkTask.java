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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.kafka.common.TopicPartition;
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
  private final ReentrantLock flushLock = new ReentrantLock();

  // Track skipped time-based flushes to ensure they eventually happen
  private volatile int consecutiveFlushSkips = 0;
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
      estimatedBytes +=
          root.getFieldVectors().stream().mapToLong(v -> v.getBufferSize()).sum();
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

    LOG.info(
        "Buffering config: flushSize={}, flushIntervalMs={}, fileSizeBytes={}",
        flushSize,
        flushIntervalMs,
        fileSizeBytes);

    // Start scheduled flush checker (runs every 1 second to check time-based flush)
    this.flushScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "ducklake-flush-scheduler");
      t.setDaemon(true);
      return t;
    });
    flushScheduler.scheduleAtFixedRate(this::checkTimeBasedFlush, 1, 1, TimeUnit.SECONDS);
  }

  /** Periodic check for time-based flush */
  private void checkTimeBasedFlush() {
    boolean lockAcquired = false;
    try {
      lockAcquired = flushLock.tryLock(FLUSH_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return;
    }

    if (!lockAcquired) {
      consecutiveFlushSkips++;
      if (consecutiveFlushSkips >= MAX_CONSECUTIVE_SKIPS_BEFORE_WARNING) {
        LOG.warn(
            "Time-based flush check skipped {} consecutive times - lock contention may delay flushes",
            consecutiveFlushSkips);
      }
      return;
    }

    try {
      consecutiveFlushSkips = 0; // Reset on successful lock acquisition
      long now = System.currentTimeMillis();
      for (Map.Entry<TopicPartition, PartitionBuffer> entry : buffers.entrySet()) {
        PartitionBuffer buffer = entry.getValue();
        if (!buffer.pendingBatches.isEmpty()
            && (now - buffer.lastFlushTime) >= flushIntervalMs) {
          TopicPartition partition = entry.getKey();
          LOG.info(
              "Time-based flush triggered for partition {} (age={}ms, records={}, bytes={})",
              partition,
              now - buffer.lastFlushTime,
              buffer.recordCount,
              buffer.estimatedBytes);
          flushPartition(partition);
        }
      }
    } catch (Exception e) {
      LOG.warn("Error during time-based flush check: {}", e.getMessage());
    } finally {
      flushLock.unlock();
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

    flushLock.lock();
    try {
      // Detect if we have Arrow IPC data (VectorSchemaRoot) or traditional data
      boolean hasArrowIpcData =
          records.stream().anyMatch(record -> record.value() instanceof VectorSchemaRoot);

      if (hasArrowIpcData) {
        bufferArrowIpcRecords(records);
      } else {
        bufferTraditionalRecords(records);
      }

      // Check if any partition needs flushing
      checkAndFlush();
    } catch (Exception e) {
      // Build a concise metadata sample for easier debugging (topic:partition@offset)
      var sb = new StringBuilder();
      sb.append("Error processing records. batchSize=")
          .append(records.size())
          .append(". sampleOffsets=");
      var idx = 0;
      for (SinkRecord r : records) {
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
      flushLock.unlock();
    }
  }

  /** Check all partitions and flush those that have exceeded thresholds */
  private void checkAndFlush() {
    for (Map.Entry<TopicPartition, PartitionBuffer> entry : buffers.entrySet()) {
      PartitionBuffer buffer = entry.getValue();
      if (buffer.shouldFlush(flushSize, fileSizeBytes, flushIntervalMs)) {
        TopicPartition partition = entry.getKey();
        String reason =
            buffer.recordCount >= flushSize
                ? "record count"
                : buffer.estimatedBytes >= fileSizeBytes ? "file size" : "time interval";
        LOG.info(
            "Flush triggered for partition {} (reason={}, records={}, bytes={})",
            partition,
            reason,
            buffer.recordCount,
            buffer.estimatedBytes);
        flushPartition(partition);
      }
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

    LOG.info(
        "Flushing partition {}: {} batches, {} records, {} bytes",
        partition,
        buffer.pendingBatches.size(),
        buffer.recordCount,
        buffer.estimatedBytes);

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

  /** Buffer records that contain VectorSchemaRoot objects (from Arrow IPC converter) */
  private void bufferArrowIpcRecords(Collection<SinkRecord> records) {
    LOG.debug("Buffering {} Arrow IPC records", records.size());

    // Group Arrow IPC records by partition
    Map<TopicPartition, List<VectorSchemaRoot>> vectorsByPartition = new HashMap<>();

    for (SinkRecord record : records) {
      if (record.value() instanceof VectorSchemaRoot vectorSchemaRoot) {
        TopicPartition partition = new TopicPartition(record.topic(), record.kafkaPartition());
        vectorsByPartition.computeIfAbsent(partition, k -> new ArrayList<>()).add(vectorSchemaRoot);
      } else {
        LOG.warn(
            "Mixed data types detected - record value is not VectorSchemaRoot: {}",
            record.value().getClass().getName());
      }
    }

    // Add to buffers
    for (Map.Entry<TopicPartition, List<VectorSchemaRoot>> entry : vectorsByPartition.entrySet()) {
      TopicPartition partition = entry.getKey();
      List<VectorSchemaRoot> vectorRoots = entry.getValue();

      PartitionBuffer buffer = buffers.get(partition);
      if (buffer == null) {
        LOG.warn("No buffer found for partition: {}", partition);
        // Close VectorSchemaRoot objects if no buffer
        for (VectorSchemaRoot root : vectorRoots) {
          try {
            root.close();
          } catch (Exception e) {
            // Ignore
          }
        }
        continue;
      }

      for (VectorSchemaRoot root : vectorRoots) {
        buffer.add(root);
      }
    }
  }

  /** Buffer traditional Kafka Connect records using the existing converter */
  private void bufferTraditionalRecords(Collection<SinkRecord> records) {
    LOG.debug("Buffering {} traditional records", records.size());

    // Group records by topic-partition
    Map<TopicPartition, List<SinkRecord>> recordsByPartition =
        SinkRecordToArrowConverter.groupRecordsByPartition(records);

    // Convert records to VectorSchemaRoot for each partition
    Map<TopicPartition, VectorSchemaRoot> vectorsByPartition =
        converter.convertRecordsByPartition(recordsByPartition);

    // Add to buffers
    for (Map.Entry<TopicPartition, VectorSchemaRoot> entry : vectorsByPartition.entrySet()) {
      TopicPartition partition = entry.getKey();
      VectorSchemaRoot vectorSchemaRoot = entry.getValue();

      PartitionBuffer buffer = buffers.get(partition);
      if (buffer == null) {
        LOG.warn("No buffer found for partition: {}", partition);
        try {
          vectorSchemaRoot.close();
        } catch (Exception e) {
          // Ignore
        }
        continue;
      }

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

    // Flush all remaining buffered data
    flushLock.lock();
    try {
      for (TopicPartition partition : buffers.keySet()) {
        try {
          flushPartition(partition);
        } catch (Exception e) {
          LOG.warn("Failed to flush partition {} during stop: {}", partition, e.getMessage());
        }
      }
      buffers.clear();
    } finally {
      flushLock.unlock();
    }

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
