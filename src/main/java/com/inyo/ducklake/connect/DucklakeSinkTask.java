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

import com.inyo.ducklake.ingestor.ArrowSchemaMerge;
import com.inyo.ducklake.ingestor.DucklakeWriter;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
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
  private Map<TopicPartition, SpillablePartitionBuffer> spillableBuffers;
  private int flushSize;
  private long flushIntervalMs;
  private long fileSizeBytes;
  private ScheduledExecutorService flushScheduler;
  private ExecutorService partitionExecutor;
  private boolean parallelPartitionFlush;

  // Spill configuration
  private boolean spillEnabled;
  private Path spillDirectory;

  // Per-partition locks for buffer operations (fast operations like adding records)
  private final ConcurrentHashMap<TopicPartition, ReentrantLock> partitionLocks =
      new ConcurrentHashMap<>();

  // Per-partition locks for flush operations (slow I/O - only one flush at a time per partition)
  private final ConcurrentHashMap<TopicPartition, ReentrantLock> flushLocks =
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
    this.spillableBuffers = new HashMap<>();
    this.allocator = new RootAllocator();
    this.converter = new SinkRecordToArrowConverter(allocator);

    // Initialize buffering configuration
    this.flushSize = config.getFlushSize();
    this.flushIntervalMs = config.getFlushIntervalMs();
    this.fileSizeBytes = config.getFileSizeBytes();
    this.parallelPartitionFlush = config.isParallelPartitionFlushEnabled();

    // Initialize spill configuration
    this.spillEnabled = config.isSpillEnabled();
    if (spillEnabled) {
      String spillDir = config.getSpillDirectory();
      if (spillDir == null || spillDir.isEmpty()) {
        // Use a fixed directory path in system temp instead of creating new temp dirs each time.
        // This prevents orphaned directories from accumulating across task restarts.
        Path tempDir = Path.of(System.getProperty("java.io.tmpdir"));
        this.spillDirectory = tempDir.resolve("ducklake-spill");

        // Clean up any orphaned ducklake-spill-* directories from previous task instances
        // that used Files.createTempDirectory() (old behavior)
        cleanupOrphanedSpillDirectories(tempDir);
      } else {
        this.spillDirectory = Path.of(spillDir);
      }

      // Ensure the spill directory exists
      try {
        Files.createDirectories(spillDirectory);
      } catch (IOException e) {
        throw new RuntimeException("Failed to create spill directory: " + spillDirectory, e);
      }
      LOG.info("Spill enabled, using directory: {}", spillDirectory);
    }

    int threadCount = config.getDuckDbThreads();
    LOG.info(
        "Buffering config: flushSize={}, flushIntervalMs={}, fileSizeBytes={}, "
            + "parallelPartitionFlush={}, duckdbThreads={}, spillEnabled={}",
        flushSize,
        flushIntervalMs,
        fileSizeBytes,
        parallelPartitionFlush,
        threadCount,
        spillEnabled);

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

    if (spillEnabled) {
      checkTimeBasedFlushSpillable(now);
    } else {
      checkTimeBasedFlushInMemory(now);
    }
  }

  /** Time-based flush check for spillable buffers */
  private void checkTimeBasedFlushSpillable(long now) {
    for (Map.Entry<TopicPartition, SpillablePartitionBuffer> entry : spillableBuffers.entrySet()) {
      TopicPartition partition = entry.getKey();
      SpillablePartitionBuffer buffer = entry.getValue();

      // Get or assign jitter for this partition
      long jitter = getPartitionJitter(partition);
      long effectiveFlushInterval = flushIntervalMs + jitter;

      // Skip if buffer is empty or not due for flush
      if (buffer.isEmpty() || (now - buffer.getLastFlushTime()) < effectiveFlushInterval) {
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
        consecutiveFlushSkips.computeIfAbsent(partition, k -> new AtomicInteger(0)).set(0);

        // Re-check condition under lock
        if (!buffer.isEmpty() && (now - buffer.getLastFlushTime()) >= effectiveFlushInterval) {
          LOG.info(
              "Time-based flush triggered for partition {} (age={}ms, jitter={}ms, records={}, bytes={})",
              partition,
              now - buffer.getLastFlushTime(),
              jitter,
              buffer.getRecordCount(),
              buffer.getEstimatedBytes());

          // Read batches from disk and flush
          List<VectorSchemaRoot> batches = buffer.readBatches(allocator);
          FlushData flushData =
              new FlushData(batches, buffer.getRecordCount(), buffer.getEstimatedBytes());
          buffer.clear();

          // Release lock before slow I/O
          lock.unlock();
          lockAcquired = false;

          try {
            flushBatches(partition, flushData);
          } catch (Exception e) {
            LOG.warn(
                "Error during time-based flush for partition {}: {}", partition, e.getMessage());
          }
        }
      } finally {
        if (lockAcquired) {
          lock.unlock();
        }
      }
    }
  }

  /** Time-based flush check for in-memory buffers */
  private void checkTimeBasedFlushInMemory(long now) {
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

      // Phase 1: Extract flush data under lock
      FlushData flushData = null;
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
          flushData = extractFlushDataFromBuffer(buffer);
        }
      } finally {
        lock.unlock();
      }

      // Phase 2: Perform slow I/O flush OUTSIDE the lock
      if (flushData != null) {
        try {
          flushBatches(partition, flushData);
        } catch (Exception e) {
          LOG.warn("Error during time-based flush for partition {}: {}", partition, e.getMessage());
        }
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
        if (spillEnabled) {
          // Create spillable buffer with partition-specific subdirectory
          Path partitionSpillDir =
              spillDirectory.resolve(partition.topic() + "-" + partition.partition());

          // Clean up any orphaned spill files from a previous task instance that may have
          // crashed or been terminated without proper cleanup during rebalancing
          if (Files.exists(partitionSpillDir)) {
            LOG.info(
                "Cleaning up existing partition spill directory before use: {}", partitionSpillDir);
            try {
              deleteDirectoryRecursively(partitionSpillDir);
            } catch (IOException e) {
              LOG.warn(
                  "Failed to clean up partition spill directory {}: {}",
                  partitionSpillDir,
                  e.getMessage());
            }
          }

          spillableBuffers.put(partition, new SpillablePartitionBuffer(partitionSpillDir));
          LOG.info("Created writer and spillable buffer for partition: {}", partition);
        } else {
          buffers.put(partition, new PartitionBuffer());
          LOG.info("Created writer and in-memory buffer for partition: {}", partition);
        }
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

  /** Data extracted from buffer for flushing outside the lock. */
  private static class FlushData {
    final List<VectorSchemaRoot> batches;
    final long recordCount;
    final long estimatedBytes;

    FlushData(List<VectorSchemaRoot> batches, long recordCount, long estimatedBytes) {
      this.batches = batches;
      this.recordCount = recordCount;
      this.estimatedBytes = estimatedBytes;
    }
  }

  /** Process a single partition with proper locking. */
  private void processPartition(TopicPartition partition, List<SinkRecord> partitionRecords) {
    if (spillEnabled) {
      processPartitionSpillable(partition, partitionRecords);
    } else {
      processPartitionInMemory(partition, partitionRecords);
    }
  }

  /** Process a single partition using spillable buffer. */
  private void processPartitionSpillable(
      TopicPartition partition, List<SinkRecord> partitionRecords) {
    ReentrantLock lock = partitionLocks.computeIfAbsent(partition, k -> new ReentrantLock());
    SpillablePartitionBuffer buffer = spillableBuffers.get(partition);

    if (buffer == null) {
      LOG.warn("No spillable buffer found for partition: {}", partition);
      return;
    }

    FlushData flushData = null;
    lock.lock();
    try {
      // Convert records to Arrow and spill to disk
      boolean hasArrowIpcData =
          partitionRecords.stream().anyMatch(record -> record.value() instanceof VectorSchemaRoot);

      if (hasArrowIpcData) {
        // Arrow IPC data - spill each VectorSchemaRoot directly
        for (SinkRecord record : partitionRecords) {
          if (record.value() instanceof VectorSchemaRoot root) {
            buffer.add(root);
          }
        }
      } else {
        // Traditional data - convert to Arrow first, then spill
        Map<TopicPartition, List<SinkRecord>> singlePartitionMap = new HashMap<>();
        singlePartitionMap.put(partition, partitionRecords);

        try {
          Map<TopicPartition, VectorSchemaRoot> vectorsByPartition =
              converter.convertRecordsByPartition(singlePartitionMap);
          VectorSchemaRoot root = vectorsByPartition.get(partition);
          if (root != null) {
            buffer.add(root); // This spills to disk and closes the root
          }
        } catch (RuntimeException e) {
          if (isSchemaConflictError(e)) {
            LOG.warn(
                "Schema conflict in spillable mode for partition {}: {}",
                partition,
                e.getMessage());
            // In spillable mode, we can't easily do per-record DLQ handling
            // because we need to write to disk. For now, just throw.
            throw e;
          }
          throw e;
        }
      }

      // Check if flush needed
      if (buffer.shouldFlush(flushSize, fileSizeBytes, flushIntervalMs)) {
        String reason;
        if (buffer.getRecordCount() >= flushSize) {
          reason = "record count";
        } else if (buffer.getEstimatedBytes() >= fileSizeBytes) {
          reason = "file size";
        } else {
          reason = "time interval";
        }
        LOG.info(
            "Flush triggered for partition {} (reason={}, records={}, bytes={})",
            partition,
            reason,
            buffer.getRecordCount(),
            buffer.getEstimatedBytes());

        // Read batches back from disk
        List<VectorSchemaRoot> batches = buffer.readBatches(allocator);
        flushData = new FlushData(batches, buffer.getRecordCount(), buffer.getEstimatedBytes());
        buffer.clear();
      }
    } catch (Exception e) {
      LOG.error("Error processing records for partition {}", partition, e);
      throw new RuntimeException("Failed to process sink records", e);
    } finally {
      lock.unlock();
    }

    // Phase 2: Perform slow I/O flush OUTSIDE the lock
    if (flushData != null) {
      flushBatches(partition, flushData);
    }
  }

  /** Process a single partition using in-memory buffer. */
  private void processPartitionInMemory(
      TopicPartition partition, List<SinkRecord> partitionRecords) {
    ReentrantLock lock = partitionLocks.computeIfAbsent(partition, k -> new ReentrantLock());

    // Phase 1: Buffer records and check if flush needed (under lock)
    FlushData flushData = null;
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

      // Check if this partition needs flushing and extract data if so
      flushData = checkAndExtractFlushData(partition);
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

    // Phase 2: Perform slow I/O flush OUTSIDE the lock
    if (flushData != null) {
      flushBatches(partition, flushData);
    }
  }

  /**
   * Check if flush is needed and extract data for flushing (called under partition lock). Returns
   * FlushData if flush is needed, null otherwise. This method extracts the batches from the buffer
   * and resets the buffer state, allowing the actual I/O to happen outside the lock.
   */
  private FlushData checkAndExtractFlushData(TopicPartition partition) {
    PartitionBuffer buffer = buffers.get(partition);
    if (buffer == null || buffer.pendingBatches.isEmpty()) {
      return null;
    }

    // Check for global memory pressure from Arrow allocator
    long allocatedMemory = allocator.getAllocatedMemory();
    boolean memoryPressure = allocatedMemory > fileSizeBytes * buffers.size();

    if (memoryPressure) {
      LOG.warn(
          "Memory pressure detected for partition {}: allocatorBytes={}, threshold={}",
          partition,
          allocatedMemory,
          fileSizeBytes * buffers.size());
    }

    // Flush if normal thresholds exceeded OR if under memory pressure with data buffered
    boolean shouldFlush =
        buffer.shouldFlush(flushSize, fileSizeBytes, flushIntervalMs) || memoryPressure;

    if (!shouldFlush) {
      return null;
    }

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

    // Extract batches and reset buffer state (still under lock)
    return extractFlushDataFromBuffer(buffer);
  }

  /**
   * Extract flush data from buffer and reset buffer state. Must be called under partition lock.
   * Returns the extracted FlushData containing batches to write.
   */
  private FlushData extractFlushDataFromBuffer(PartitionBuffer buffer) {
    // Move batches out of buffer
    List<VectorSchemaRoot> batches = new ArrayList<>(buffer.pendingBatches);
    long recordCount = buffer.recordCount;
    long estimatedBytes = buffer.estimatedBytes;

    // Clear buffer state (but don't close the batches - they'll be closed after flush)
    buffer.pendingBatches.clear();
    buffer.recordCount = 0;
    buffer.estimatedBytes = 0;
    buffer.lastFlushTime = System.currentTimeMillis();

    return new FlushData(batches, recordCount, estimatedBytes);
  }

  /**
   * Perform the actual flush I/O operation. This method does NOT require holding the partition
   * buffer lock, allowing buffer operations to proceed while the slow I/O happens. However, it
   * acquires a separate flush lock to ensure only one flush happens at a time per partition,
   * preventing concurrent writes to DuckLake which could cause metadata inconsistencies.
   */
  private void flushBatches(TopicPartition partition, FlushData flushData) {
    DucklakeWriter writer = writers.get(partition);
    if (writer == null) {
      LOG.warn(
          "No writer found for partition: {}, closing {} batches",
          partition,
          flushData.batches.size());
      closeBatches(flushData.batches);
      return;
    }

    // Acquire flush lock to ensure only one flush at a time per partition
    // This prevents concurrent DuckLake writes which could cause metadata inconsistencies
    ReentrantLock flushLock = flushLocks.computeIfAbsent(partition, k -> new ReentrantLock());
    flushLock.lock();
    try {
      long actualAllocatedBytes = allocator.getAllocatedMemory();
      LOG.info(
          "Flushing partition {}: {} batches, {} records, estimatedBytes={}, allocatorBytes={}",
          partition,
          flushData.batches.size(),
          flushData.recordCount,
          flushData.estimatedBytes,
          actualAllocatedBytes);

      VectorSchemaRoot consolidatedRoot = null;
      try {
        consolidatedRoot = consolidateBatches(flushData.batches);
        if (consolidatedRoot != null && consolidatedRoot.getRowCount() > 0) {
          // Consolidated successfully - write single batch
          writer.write(consolidatedRoot);
        } else if (consolidatedRoot == null && !flushData.batches.isEmpty()) {
          // Consolidation failed (schema mismatch) - fall back to writing individually
          LOG.info(
              "Writing {} batches individually for partition {} due to schema differences",
              flushData.batches.size(),
              partition);
          for (VectorSchemaRoot root : flushData.batches) {
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
        closeBatches(flushData.batches);
      }
    } finally {
      flushLock.unlock();
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
   * Flush all buffered data for a partition. This method is used during stop() where the lock is
   * already held. It extracts the data and performs the flush synchronously.
   */
  private void flushPartition(TopicPartition partition) {
    PartitionBuffer buffer = buffers.get(partition);
    if (buffer == null || buffer.pendingBatches.isEmpty()) {
      return;
    }

    // Extract flush data and perform flush
    FlushData flushData = extractFlushDataFromBuffer(buffer);
    flushBatches(partition, flushData);
  }

  /**
   * Flush all spillable buffered data for a partition. This method is used during stop() where the
   * lock is already held. It reads batches from disk and performs the flush synchronously.
   */
  private void flushSpillablePartition(TopicPartition partition) {
    SpillablePartitionBuffer buffer = spillableBuffers.get(partition);
    if (buffer == null || buffer.isEmpty()) {
      return;
    }

    LOG.info(
        "Flushing spillable partition {} during stop: {} batches, {} records",
        partition,
        buffer.getBatchCount(),
        buffer.getRecordCount());

    // Read batches from disk and flush
    List<VectorSchemaRoot> batches = buffer.readBatches(allocator);
    FlushData flushData =
        new FlushData(batches, buffer.getRecordCount(), buffer.getEstimatedBytes());
    buffer.clear();
    flushBatches(partition, flushData);
  }

  /**
   * Cleans up orphaned ducklake-spill-* directories in the temp directory. These directories may
   * have been left behind by previous task instances that crashed or were terminated without proper
   * cleanup. Only cleans up directories matching the old temp directory naming pattern
   * (ducklake-spill*).
   *
   * <p>Package-private for testing.
   */
  void cleanupOrphanedSpillDirectories(Path tempDir) {
    try (Stream<Path> paths = Files.list(tempDir)) {
      paths
          .filter(Files::isDirectory)
          .filter(
              p -> {
                String name = p.getFileName().toString();
                // Match old-style temp directories: ducklake-spill followed by random suffix
                // e.g., ducklake-spill1234567890
                // But NOT our new fixed directory name "ducklake-spill" (exact match)
                return name.startsWith("ducklake-spill") && !name.equals("ducklake-spill");
              })
          .forEach(
              orphanedDir -> {
                LOG.info("Cleaning up orphaned spill directory: {}", orphanedDir);
                try {
                  deleteDirectoryRecursively(orphanedDir);
                } catch (IOException e) {
                  LOG.warn(
                      "Failed to clean up orphaned spill directory {}: {}",
                      orphanedDir,
                      e.getMessage());
                }
              });
    } catch (IOException e) {
      LOG.warn("Failed to scan for orphaned spill directories in {}: {}", tempDir, e.getMessage());
    }
  }

  /** Recursively delete a directory and all its contents. */
  private void deleteDirectoryRecursively(Path directory) throws IOException {
    if (directory == null || !Files.exists(directory)) {
      return;
    }
    Files.walk(directory)
        .sorted(
            java.util.Comparator
                .reverseOrder()) // Reverse order so files are deleted before directories
        .forEach(
            path -> {
              try {
                Files.deleteIfExists(path);
              } catch (IOException e) {
                LOG.warn("Failed to delete {}: {}", path, e.getMessage());
              }
            });
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
   * Casts a VectorSchemaRoot to a target schema. This handles: - Missing fields: filled with nulls
   * - Type promotions: Int32 to Int64, Float32 to Float64 - Field reordering: fields are matched by
   * name
   *
   * @param source The source batch to cast
   * @param targetSchema The target schema to cast to
   * @return A new VectorSchemaRoot with the target schema, or null if casting fails
   */
  private VectorSchemaRoot castBatchToSchema(VectorSchemaRoot source, Schema targetSchema) {
    if (source.getSchema().equals(targetSchema)) {
      // Schemas are identical - just copy
      VectorSchemaRoot copy = VectorSchemaRoot.create(targetSchema, allocator);
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
        try {
          copy.close();
        } catch (Exception closeEx) {
          LOG.warn("Failed to close copy during cleanup: {}", closeEx.getMessage());
        }
        throw e;
      }
    }

    // Build a map of source fields by name for quick lookup
    Map<String, FieldVector> sourceVectorsByName = new HashMap<>();
    for (FieldVector vector : source.getFieldVectors()) {
      sourceVectorsByName.put(vector.getName(), vector);
    }

    VectorSchemaRoot result = VectorSchemaRoot.create(targetSchema, allocator);
    try {
      result.allocateNew();
      int rowCount = source.getRowCount();

      for (FieldVector targetVector : result.getFieldVectors()) {
        String fieldName = targetVector.getName();
        FieldVector sourceVector = sourceVectorsByName.get(fieldName);

        if (sourceVector == null) {
          // Field doesn't exist in source - fill with nulls
          for (int row = 0; row < rowCount; row++) {
            targetVector.setNull(row);
          }
        } else if (sourceVector.getField().getType().equals(targetVector.getField().getType())) {
          // Same type - direct copy
          for (int row = 0; row < rowCount; row++) {
            targetVector.copyFromSafe(row, row, sourceVector);
          }
        } else {
          // Different types - need to cast
          if (!castVectorValues(sourceVector, targetVector, rowCount)) {
            // Casting failed - clean up and return null
            try {
              result.close();
            } catch (Exception closeEx) {
              LOG.warn("Failed to close result during cleanup: {}", closeEx.getMessage());
            }
            return null;
          }
        }
      }

      result.setRowCount(rowCount);
      return result;
    } catch (Exception e) {
      try {
        result.close();
      } catch (Exception closeEx) {
        LOG.warn("Failed to close result during cleanup: {}", closeEx.getMessage());
      }
      throw e;
    }
  }

  /**
   * Casts values from source vector to target vector. Handles common type promotions.
   *
   * @return true if casting succeeded, false if types are incompatible
   */
  private boolean castVectorValues(FieldVector source, FieldVector target, int rowCount) {
    ArrowType sourceType = source.getField().getType();
    ArrowType targetType = target.getField().getType();

    // TinyInt (Int8) to Int64 promotion
    if (source instanceof TinyIntVector tinyIntSource
        && target instanceof BigIntVector bigIntTarget) {
      for (int row = 0; row < rowCount; row++) {
        if (tinyIntSource.isNull(row)) {
          bigIntTarget.setNull(row);
        } else {
          bigIntTarget.setSafe(row, tinyIntSource.get(row));
        }
      }
      return true;
    }

    // SmallInt (Int16) to Int64 promotion
    if (source instanceof SmallIntVector smallIntSource
        && target instanceof BigIntVector bigIntTarget) {
      for (int row = 0; row < rowCount; row++) {
        if (smallIntSource.isNull(row)) {
          bigIntTarget.setNull(row);
        } else {
          bigIntTarget.setSafe(row, smallIntSource.get(row));
        }
      }
      return true;
    }

    // Int32 to Int64 promotion
    if (source instanceof IntVector intSource && target instanceof BigIntVector bigIntTarget) {
      for (int row = 0; row < rowCount; row++) {
        if (intSource.isNull(row)) {
          bigIntTarget.setNull(row);
        } else {
          bigIntTarget.setSafe(row, intSource.get(row));
        }
      }
      return true;
    }

    // Float32 to Float64 promotion
    if (source instanceof Float4Vector float4Source
        && target instanceof Float8Vector float8Target) {
      for (int row = 0; row < rowCount; row++) {
        if (float4Source.isNull(row)) {
          float8Target.setNull(row);
        } else {
          float8Target.setSafe(row, float4Source.get(row));
        }
      }
      return true;
    }

    // TinyInt to Float64 promotion
    if (source instanceof TinyIntVector tinyIntSource
        && target instanceof Float8Vector float8Target) {
      for (int row = 0; row < rowCount; row++) {
        if (tinyIntSource.isNull(row)) {
          float8Target.setNull(row);
        } else {
          float8Target.setSafe(row, tinyIntSource.get(row));
        }
      }
      return true;
    }

    // SmallInt to Float64 promotion
    if (source instanceof SmallIntVector smallIntSource
        && target instanceof Float8Vector float8Target) {
      for (int row = 0; row < rowCount; row++) {
        if (smallIntSource.isNull(row)) {
          float8Target.setNull(row);
        } else {
          float8Target.setSafe(row, smallIntSource.get(row));
        }
      }
      return true;
    }

    // Int32 to Float64 promotion
    if (source instanceof IntVector intSource && target instanceof Float8Vector float8Target) {
      for (int row = 0; row < rowCount; row++) {
        if (intSource.isNull(row)) {
          float8Target.setNull(row);
        } else {
          float8Target.setSafe(row, intSource.get(row));
        }
      }
      return true;
    }

    // Int64 to Float64 promotion
    if (source instanceof BigIntVector bigIntSource
        && target instanceof Float8Vector float8Target) {
      for (int row = 0; row < rowCount; row++) {
        if (bigIntSource.isNull(row)) {
          float8Target.setNull(row);
        } else {
          float8Target.setSafe(row, bigIntSource.get(row));
        }
      }
      return true;
    }

    // For other compatible types, try copyFromSafe (works for same-type with different nullability)
    try {
      for (int row = 0; row < rowCount; row++) {
        target.copyFromSafe(row, row, source);
      }
      return true;
    } catch (Exception e) {
      LOG.debug("Cannot cast from {} to {}: {}", sourceType, targetType, e.getMessage());
      return false;
    }
  }

  /**
   * Consolidates multiple VectorSchemaRoot batches into a single VectorSchemaRoot. This reduces the
   * number of write operations to DuckLake, improving throughput.
   *
   * <p>If batches have different schemas, attempts to unify them using ArrowSchemaMerge and cast
   * each batch to the unified schema. Returns null only if schema unification fails (caller should
   * fall back to writing individually).
   */
  private VectorSchemaRoot consolidateBatches(List<VectorSchemaRoot> batches) {
    if (batches == null || batches.isEmpty()) {
      return null;
    }

    // Fast path: if all schemas are identical, skip unification
    boolean schemasMatch = haveSameSchema(batches);

    Schema unifiedSchema;
    List<VectorSchemaRoot> unifiedBatches;

    if (schemasMatch) {
      // All schemas match - use as-is
      unifiedSchema = batches.get(0).getSchema();
      unifiedBatches = batches;
    } else {
      // Schemas differ - try to unify them
      List<Schema> schemas =
          batches.stream().map(VectorSchemaRoot::getSchema).collect(Collectors.toList());

      try {
        unifiedSchema = ArrowSchemaMerge.unifySchemas(schemas, () -> Map.of());
        LOG.debug(
            "Unified {} different schemas into single schema with {} fields",
            batches.size(),
            unifiedSchema.getFields().size());
      } catch (IllegalArgumentException e) {
        // Schema unification failed - truly incompatible types
        LOG.warn(
            "Cannot unify {} batch schemas - will write individually: {}",
            batches.size(),
            e.getMessage());
        return null;
      }

      // Cast each batch to the unified schema
      unifiedBatches = new ArrayList<>(batches.size());
      List<VectorSchemaRoot> batchesToClose = new ArrayList<>();
      try {
        for (VectorSchemaRoot batch : batches) {
          VectorSchemaRoot castedBatch = castBatchToSchema(batch, unifiedSchema);
          if (castedBatch == null) {
            // Casting failed - clean up and fall back to individual writes
            LOG.warn("Failed to cast batch to unified schema - will write individually");
            for (VectorSchemaRoot toClose : batchesToClose) {
              try {
                toClose.close();
              } catch (Exception closeEx) {
                LOG.warn("Failed to close casted batch during cleanup: {}", closeEx.getMessage());
              }
            }
            return null;
          }
          unifiedBatches.add(castedBatch);
          batchesToClose.add(castedBatch);
        }
      } catch (Exception e) {
        // Clean up any casted batches on failure
        for (VectorSchemaRoot toClose : batchesToClose) {
          try {
            toClose.close();
          } catch (Exception closeEx) {
            LOG.warn("Failed to close casted batch during cleanup: {}", closeEx.getMessage());
          }
        }
        throw e;
      }
    }

    // Handle single batch case
    if (unifiedBatches.size() == 1) {
      VectorSchemaRoot source = unifiedBatches.get(0);
      if (schemasMatch) {
        // Need to copy since original will be closed separately
        VectorSchemaRoot copy = VectorSchemaRoot.create(unifiedSchema, allocator);
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
          try {
            copy.close();
          } catch (Exception closeEx) {
            LOG.warn("Failed to close copy root during cleanup: {}", closeEx.getMessage());
          }
          throw e;
        }
      } else {
        // Already casted - just return it (caller will close original)
        return source;
      }
    }

    // Calculate total row count
    int totalRows = unifiedBatches.stream().mapToInt(VectorSchemaRoot::getRowCount).sum();

    // Create consolidated root
    VectorSchemaRoot consolidated = VectorSchemaRoot.create(unifiedSchema, allocator);
    try {
      consolidated.allocateNew();

      // Copy data from each batch using VectorBatchAppender
      VectorSchemaRoot[] batchArray = unifiedBatches.toArray(new VectorSchemaRoot[0]);
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
          "Consolidated {} batches into single batch with {} rows",
          unifiedBatches.size(),
          totalRows);

      // Close casted batches if we created them (not the originals)
      if (!schemasMatch) {
        for (VectorSchemaRoot castedBatch : unifiedBatches) {
          try {
            castedBatch.close();
          } catch (Exception closeEx) {
            LOG.warn("Failed to close casted batch: {}", closeEx.getMessage());
          }
        }
      }

      return consolidated;
    } catch (Exception e) {
      // Clean up on failure
      try {
        consolidated.close();
      } catch (Exception closeEx) {
        LOG.warn("Failed to close consolidated root during cleanup: {}", closeEx.getMessage());
      }
      // Also clean up casted batches if we created them
      if (!schemasMatch) {
        for (VectorSchemaRoot castedBatch : unifiedBatches) {
          try {
            castedBatch.close();
          } catch (Exception closeEx) {
            LOG.warn("Failed to close casted batch during cleanup: {}", closeEx.getMessage());
          }
        }
      }
      throw e;
    }
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
    if (spillEnabled) {
      // Flush spillable buffers
      for (TopicPartition partition : spillableBuffers.keySet()) {
        ReentrantLock lock = partitionLocks.get(partition);
        if (lock != null) {
          lock.lock();
        }
        try {
          flushSpillablePartition(partition);
        } catch (Exception e) {
          LOG.warn(
              "Failed to flush spillable partition {} during stop: {}", partition, e.getMessage());
        } finally {
          if (lock != null) {
            lock.unlock();
          }
        }
      }
      spillableBuffers.clear();
      // Clean up spill directory
      if (spillDirectory != null) {
        try {
          deleteDirectoryRecursively(spillDirectory);
          LOG.info("Cleaned up spill directory: {}", spillDirectory);
        } catch (Exception e) {
          LOG.warn("Failed to clean up spill directory {}: {}", spillDirectory, e.getMessage());
        }
      }
    } else {
      // Flush in-memory buffers
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
    }
    partitionLocks.clear();
    flushLocks.clear();
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
