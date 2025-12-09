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
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;

public class DucklakeSinkTask extends SinkTask {
  private static final System.Logger LOG = System.getLogger(String.valueOf(DucklakeSinkTask.class));
  private DucklakeSinkConfig config;
  private DucklakeConnectionFactory connectionFactory;
  private DucklakeWriterFactory writerFactory;
  private Map<TopicPartition, DucklakeWriter> writers;
  private BufferAllocator allocator;
  private SinkRecordToArrowConverter converter;
  private DucklakeMetrics metrics;
  private SinkTaskContext context;

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
    this.allocator = new RootAllocator();
    this.converter = new SinkRecordToArrowConverter(allocator);

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

      // Create one writer for each partition
      for (TopicPartition partition : partitions) {
        DucklakeWriter writer = writerFactory.create(partition.topic());
        writers.put(partition, writer);
        LOG.log(System.Logger.Level.INFO, "Created writer for partition: {0}", partition);
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

    try {
      // Record batch size metric
      metrics.recordBatchProcessed(records.size());

      // Detect if we have Arrow IPC data (VectorSchemaRoot) or traditional data
      boolean hasArrowIpcData =
          records.stream().anyMatch(record -> record.value() instanceof VectorSchemaRoot);

      if (hasArrowIpcData) {
        processArrowIpcRecords(records);
      } else {
        processTraditionalRecords(records);
      }
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
      LOG.log(System.Logger.Level.ERROR, sb.toString(), e);
      throw new RuntimeException("Failed to process sink records", e);
    }
  }

  /** Process records that contain VectorSchemaRoot objects (from Arrow IPC converter) */
  private void processArrowIpcRecords(Collection<SinkRecord> records) {
    LOG.log(System.Logger.Level.INFO, "Processing {0} Arrow IPC records", records.size());

    // Group Arrow IPC records by partition
    Map<TopicPartition, List<VectorSchemaRoot>> vectorsByPartition = new HashMap<>();

    for (SinkRecord record : records) {
      if (record.value() instanceof VectorSchemaRoot vectorSchemaRoot) {
        TopicPartition partition = new TopicPartition(record.topic(), record.kafkaPartition());
        vectorsByPartition.computeIfAbsent(partition, k -> new ArrayList<>()).add(vectorSchemaRoot);
      } else {
        LOG.log(
            System.Logger.Level.WARNING,
            "Mixed data types detected - record value is not VectorSchemaRoot: {0}",
            record.value().getClass().getName());
      }
    }

    // Process each partition
    for (Map.Entry<TopicPartition, List<VectorSchemaRoot>> entry : vectorsByPartition.entrySet()) {
      TopicPartition partition = entry.getKey();
      List<VectorSchemaRoot> vectorRoots = entry.getValue();

      DucklakeWriter writer = writers.get(partition);
      if (writer == null) {
        LOG.log(System.Logger.Level.WARNING, "No writer found for partition: {0}", partition);
        // Close VectorSchemaRoot objects even if no writer is found
        for (VectorSchemaRoot vectorSchemaRoot : vectorRoots) {
          try {
            vectorSchemaRoot.close();
          } catch (Exception e) {
            LOG.log(
                System.Logger.Level.WARNING,
                "Failed to close VectorSchemaRoot for partition {0}: {1}",
                partition,
                e.getMessage());
          }
        }
        continue;
      }

      // Write each VectorSchemaRoot and ensure proper cleanup
      for (VectorSchemaRoot vectorSchemaRoot : vectorRoots) {
        try (vectorSchemaRoot) {
          try {
            writer.write(vectorSchemaRoot);

            LOG.log(
                System.Logger.Level.DEBUG,
                "Processed Arrow IPC data with {0} rows for partition {1}",
                vectorSchemaRoot.getRowCount(),
                partition);
          } catch (Exception e) {
            LOG.log(
                System.Logger.Level.ERROR,
                "Failed to write Arrow IPC data for partition: " + partition,
                e);
            throw new RuntimeException("Failed to write Arrow IPC data", e);
          }
        } catch (Exception e) {
          LOG.log(
              System.Logger.Level.WARNING,
              "Failed to close VectorSchemaRoot for partition {0}: {1}",
              partition,
              e.getMessage());
        }
        // Always close VectorSchemaRoot to prevent memory leaks
      }
    }
  }

  /** Process traditional Kafka Connect records using the existing converter */
  private void processTraditionalRecords(Collection<SinkRecord> records) {
    LOG.log(System.Logger.Level.DEBUG, "Processing {0} traditional records", records.size());

    // Group records by topic-partition
    Map<TopicPartition, List<SinkRecord>> recordsByPartition =
        SinkRecordToArrowConverter.groupRecordsByPartition(records);

    // Convert records to VectorSchemaRoot for each partition
    Map<TopicPartition, VectorSchemaRoot> vectorsByPartition =
        converter.convertRecordsByPartition(recordsByPartition);

    // Process each partition
    for (Map.Entry<TopicPartition, VectorSchemaRoot> entry : vectorsByPartition.entrySet()) {
      TopicPartition partition = entry.getKey();
      VectorSchemaRoot vectorSchemaRoot = entry.getValue();

      DucklakeWriter writer = writers.get(partition);
      if (writer == null) {
        LOG.log(System.Logger.Level.WARNING, "No writer found for partition: {0}", partition);
        // Ensure VectorSchemaRoot is closed even when no writer is found
        try {
          vectorSchemaRoot.close();
        } catch (Exception e) {
          LOG.log(
              System.Logger.Level.WARNING,
              "Failed to close VectorSchemaRoot for partition {0}: {1}",
              partition,
              e.getMessage());
        }
        continue;
      }

      try {
        writer.write(vectorSchemaRoot);

        LOG.log(
            System.Logger.Level.DEBUG,
            "Processed {0} traditional records for partition {1}",
            vectorSchemaRoot.getRowCount(),
            partition);
      } finally {
        // Always close the VectorSchemaRoot to free memory (from traditional converter)
        try {
          vectorSchemaRoot.close();
        } catch (Exception e) {
          LOG.log(
              System.Logger.Level.WARNING,
              "Failed to close VectorSchemaRoot for partition {0}: {1}",
              partition,
              e.getMessage());
        }
      }
    }
  }

  @Override
  public void stop() {
    try {
      if (writers != null) {
        for (DucklakeWriter w : writers.values()) {
          try {
            w.close();
          } catch (Exception e) {
            LOG.log(System.Logger.Level.WARNING, "Failed closing writer: {0}", e.getMessage());
          }
        }
        writers.clear();
        LOG.log(System.Logger.Level.INFO, "Cleared all writers");
      }
      if (converter != null) {
        try {
          converter.close();
        } catch (Exception e) {
          LOG.log(System.Logger.Level.WARNING, "Failed closing converter: {0}", e.getMessage());
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
