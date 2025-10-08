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

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Connect Converter implementation for Arrow IPC format.
 *
 * <p>This converter can serialize VectorSchemaRoot objects to Arrow IPC bytes and deserialize Arrow
 * IPC bytes back to VectorSchemaRoot objects with corresponding Kafka Connect schemas.
 */
public class ArrowIpcConverter implements Converter {

  private static final Logger LOG = LoggerFactory.getLogger(ArrowIpcConverter.class);

  private BufferAllocator allocator;
  private ArrowToKafkaConverter arrowToKafkaConverter;

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    // Initialize Arrow memory allocator
    this.allocator = new RootAllocator(Long.MAX_VALUE);
    this.arrowToKafkaConverter = new ArrowToKafkaConverter(allocator);

    LOG.info(
        "ArrowIpcConverter configured for {} with configs: {}", isKey ? "key" : "value", configs);
  }

  @Override
  public byte[] fromConnectData(String topic, Schema schema, Object value) {
    if (value == null) {
      return null;
    }

    if (!(value instanceof VectorSchemaRoot)) {
      throw new DataException(
          "ArrowIpcConverter can only convert VectorSchemaRoot objects, but received: "
              + value.getClass().getName());
    }

    try {
      var vectorSchemaRoot = (VectorSchemaRoot) value;
      return convertVectorSchemaRootToBytes(vectorSchemaRoot);
    } catch (Exception e) {
      LOG.error("Failed to convert VectorSchemaRoot to Arrow IPC bytes for topic: {}", topic, e);
      throw new DataException("Failed to serialize VectorSchemaRoot to Arrow IPC", e);
    }
  }

  @Override
  public SchemaAndValue toConnectData(String topic, byte[] value) {
    if (value == null || value.length == 0) {
      return new SchemaAndValue(null, null);
    }

    try {
      return arrowToKafkaConverter.convertFromArrowIPC(value);
    } catch (Exception e) {
      LOG.error("Failed to convert Arrow IPC bytes to SchemaAndValue for topic: {}", topic, e);
      throw new DataException("Failed to deserialize Arrow IPC data", e);
    }
  }

  /** Convert VectorSchemaRoot to Arrow IPC bytes */
  private byte[] convertVectorSchemaRootToBytes(VectorSchemaRoot root) throws Exception {
    var outputStream = new ByteArrayOutputStream();
    var channel = Channels.newChannel(outputStream);

    try (var writer = new ArrowStreamWriter(root, null, channel)) {
      writer.start();
      if (root.getRowCount() > 0) {
        writer.writeBatch();
      }
      writer.end();
    }

    return outputStream.toByteArray();
  }

  /** Close resources when the converter is no longer needed */
  public void close() {
    try {
      if (arrowToKafkaConverter != null) {
        arrowToKafkaConverter.close();
      }
    } catch (Exception e) {
      LOG.warn("Failed to close ArrowToKafkaConverter", e);
    }

    try {
      if (allocator != null) {
        allocator.close();
      }
    } catch (Exception e) {
      LOG.warn("Failed to close BufferAllocator", e);
    }
  }
}
