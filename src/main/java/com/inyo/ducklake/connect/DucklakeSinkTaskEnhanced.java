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
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

/**
 * Enhanced DucklakeSinkTask that supports both traditional Kafka Connect data and Arrow IPC data
 * through VectorSchemaRoot values.
 */
public class DucklakeSinkTaskEnhanced extends SinkTask {
  private static final System.Logger LOG =
      System.getLogger(String.valueOf(DucklakeSinkTaskEnhanced.class));

  private DucklakeSinkConfig config;
  private DucklakeConnectionFactory connectionFactory;
  private DucklakeWriterFactory writerFactory;
  private Map<TopicPartition, DucklakeWriter> writers;
  private BufferAllocator allocator;
  private SinkRecordToArrowConverter converter;

  @Override
  public String version() {
    return DucklakeSinkConfig.VERSION;
  }

  @Override
  public void start(Map<String, String> map) {
    this.config = new DucklakeSinkConfig(DucklakeSinkConfig.CONFIG_DEF, map);
    this.connectionFactory = new DucklakeConnectionFactory(config);
    this.writers = new HashMap<>();
    this.allocator = new RootAllocator();
    this.converter = new SinkRecordToArrowConverter(allocator);
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
      // Detect if we have Arrow IPC data (VectorSchemaRoot) or traditional data
      boolean hasArrowIpcData =
          records.stream().anyMatch(record -> record.value() instanceof VectorSchemaRoot);

      if (hasArrowIpcData) {
        processArrowIpcRecords(records);
      } else {
        processTraditionalRecords(records);
      }
    } catch (Exception e) {
      LOG.log(System.Logger.Level.ERROR, "Error processing records", e);
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
        continue;
      }

      // Write each VectorSchemaRoot directly
      for (VectorSchemaRoot vectorSchemaRoot : vectorRoots) {
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
        // Note: VectorSchemaRoot from Arrow IPC converter should be managed by the converter
        // We don't close it here as it might be reused
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
        vectorSchemaRoot.close();
      }
    }
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    super.close(partitions);

    // Close writers for these partitions
    for (TopicPartition partition : partitions) {
      DucklakeWriter writer = writers.remove(partition);
      if (writer != null) {
        try {
          writer.close();
          LOG.log(System.Logger.Level.INFO, "Closed writer for partition: {0}", partition);
        } catch (Exception e) {
          LOG.log(
              System.Logger.Level.WARNING, "Failed to close writer for partition: " + partition, e);
        }
      }
    }
  }

  @Override
  public void stop() {
    // Close all remaining writers
    for (Map.Entry<TopicPartition, DucklakeWriter> entry : writers.entrySet()) {
      try {
        entry.getValue().close();
        LOG.log(System.Logger.Level.INFO, "Closed writer for partition: {0}", entry.getKey());
      } catch (Exception e) {
        LOG.log(
            System.Logger.Level.WARNING,
            "Failed to close writer for partition: " + entry.getKey(),
            e);
      }
    }
    writers.clear();

    // Close converter
    if (converter != null) {
      try {
        converter.close();
      } catch (Exception e) {
        LOG.log(System.Logger.Level.WARNING, "Failed to close converter", e);
      }
    }

    // Close allocator
    if (allocator != null) {
      try {
        allocator.close();
      } catch (Exception e) {
        LOG.log(System.Logger.Level.WARNING, "Failed to close allocator", e);
      }
    }

    // Close connection factory
    if (connectionFactory != null) {
      try {
        connectionFactory.close();
      } catch (Exception e) {
        LOG.log(System.Logger.Level.WARNING, "Failed to close connection factory", e);
      }
    }
  }
}
