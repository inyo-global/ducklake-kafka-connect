package com.inyo.ducklake.connect;

import com.inyo.ducklake.ingestor.DucklakeWriter;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.sql.SQLException;
import java.util.*;

public class DucklakeSinkTask extends SinkTask {
    private static final System.Logger LOG = System.getLogger(String.valueOf(DucklakeSinkTask.class));
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
    public void put(Collection<SinkRecord> records) {
        if (records == null || records.isEmpty()) {
            return;
        }

        try {
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
                    // TODO: Pass vectorSchemaRoot to writer for processing
                    // writer.write(vectorSchemaRoot);

                    LOG.log(System.Logger.Level.INFO,
                        "Processed {0} records for partition {1}",
                        vectorSchemaRoot.getRowCount(), partition);
                } finally {
                    // Always close the VectorSchemaRoot to free memory
                    vectorSchemaRoot.close();
                }
            }
        } catch (Exception e) {
            LOG.log(System.Logger.Level.ERROR, "Error processing records", e);
            throw new RuntimeException("Failed to process sink records", e);
        }
    }

    @Override
    public void stop() {
        try {
            if (writers != null) {
                for (DucklakeWriter w : writers.values()) {
                    try { w.close(); } catch (Exception e) { LOG.log(System.Logger.Level.WARNING, "Failed closing writer: {0}", e.getMessage()); }
                }
                writers.clear();
                LOG.log(System.Logger.Level.INFO, "Cleared all writers");
            }
            if (converter != null) {
                try { converter.close(); } catch (Exception e) { LOG.log(System.Logger.Level.WARNING, "Failed closing converter: {0}", e.getMessage()); }
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
