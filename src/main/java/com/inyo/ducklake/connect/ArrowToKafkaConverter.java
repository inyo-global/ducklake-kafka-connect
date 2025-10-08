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
import java.io.ByteArrayInputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArrowToKafkaConverter {

  private static final Logger LOG = LoggerFactory.getLogger(ArrowToKafkaConverter.class);

  private final BufferAllocator allocator;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public ArrowToKafkaConverter(BufferAllocator allocator) {
    // BufferAllocator is required for Arrow operations and is typically managed externally
    // Creating a copy is not practical as it's a complex resource-managed object
    this.allocator = allocator;
  }

  /** Convert Arrow IPC bytes to Kafka Connect SchemaAndValue with VectorSchemaRoot as value */
  public SchemaAndValue convertFromArrowIPC(byte[] arrowIpcBytes) {
    if (arrowIpcBytes == null || arrowIpcBytes.length == 0) {
      LOG.warn("Empty or null Arrow IPC bytes provided");
      return new SchemaAndValue(null, null);
    }

    try (var inputStream = new ByteArrayInputStream(arrowIpcBytes);
        var channel = Channels.newChannel(inputStream);
        var reader = new ArrowStreamReader(channel, allocator)) {

      // Read the schema from the Arrow stream
      var arrowSchema = reader.getVectorSchemaRoot().getSchema();

      // Convert Arrow schema to Kafka Connect schema
      var kafkaSchema = convertArrowSchemaToKafka(arrowSchema);

      // Read all batches into a single VectorSchemaRoot
      var vectorSchemaRoot = readAllBatches(reader);

      return new SchemaAndValue(kafkaSchema, vectorSchemaRoot);

    } catch (Exception e) {
      LOG.error("Failed to convert Arrow IPC to Kafka Connect format", e);
      throw new RuntimeException("Failed to convert Arrow IPC data", e);
    }
  }

  /** Read all batches from the Arrow stream reader and combine into a single VectorSchemaRoot */
  private VectorSchemaRoot readAllBatches(ArrowStreamReader reader) throws Exception {
    var schema = reader.getVectorSchemaRoot().getSchema();
    var combinedRoot = VectorSchemaRoot.create(schema, allocator);

    List<VectorSchemaRoot> batches = new ArrayList<>();

    // Read all batches
    while (reader.loadNextBatch()) {
      var currentRoot = reader.getVectorSchemaRoot();
      if (currentRoot.getRowCount() > 0) {
        // Create a copy of the current batch
        var batchCopy = VectorSchemaRoot.create(schema, allocator);
        batchCopy.allocateNew();

        // Copy data from current batch to our copy
        for (int i = 0; i < currentRoot.getFieldVectors().size(); i++) {
          var sourceVector = currentRoot.getVector(i);
          var targetVector = batchCopy.getVector(i);
          targetVector.allocateNew();
          // Fix copyFromSafe method call - use correct signature
          for (int j = 0; j < currentRoot.getRowCount(); j++) {
            targetVector.copyFromSafe(j, j, sourceVector);
          }
        }
        batchCopy.setRowCount(currentRoot.getRowCount());
        batches.add(batchCopy);
      }
    }

    if (batches.isEmpty()) {
      return combinedRoot;
    }

    if (batches.size() == 1) {
      return batches.get(0);
    }

    // Combine multiple batches into one
    int totalRows = batches.stream().mapToInt(VectorSchemaRoot::getRowCount).sum();
    combinedRoot.allocateNew();

    int currentOffset = 0;
    for (var batch : batches) {
      for (int i = 0; i < batch.getFieldVectors().size(); i++) {
        var sourceVector = batch.getVector(i);
        var targetVector = combinedRoot.getVector(i);
        // Fix copyFromSafe method call - copy each row individually
        for (int j = 0; j < batch.getRowCount(); j++) {
          targetVector.copyFromSafe(currentOffset + j, j, sourceVector);
        }
      }
      currentOffset += batch.getRowCount();
      batch.close();
    }

    combinedRoot.setRowCount(totalRows);
    return combinedRoot;
  }

  /** Convert Arrow schema to Kafka Connect schema */
  private org.apache.kafka.connect.data.Schema convertArrowSchemaToKafka(Schema arrowSchema) {
    var schemaBuilder = SchemaBuilder.struct();

    for (Field field : arrowSchema.getFields()) {
      var kafkaField = convertArrowFieldToKafka(field);
      schemaBuilder.field(field.getName(), kafkaField);
    }

    return schemaBuilder.build();
  }

  /** Convert Arrow field to Kafka Connect schema */
  private org.apache.kafka.connect.data.Schema convertArrowFieldToKafka(Field arrowField) {
    var arrowType = arrowField.getType();
    boolean isOptional = arrowField.isNullable();

    // Handle timestamp logical types
    if (arrowType instanceof ArrowType.Timestamp timestampType) {
      var builder = SchemaBuilder.int64();
      if (timestampType.getUnit() == TimeUnit.MILLISECOND) {
        builder.name("org.apache.kafka.connect.data.Timestamp");
      }
      return isOptional ? builder.optional().build() : builder.build();
    }

    // Handle date logical types
    if (arrowType instanceof ArrowType.Date) {
      var builder = SchemaBuilder.int32().name("org.apache.kafka.connect.data.Date");
      return isOptional ? builder.optional().build() : builder.build();
    }

    // Handle time logical types
    if (arrowType instanceof ArrowType.Time) {
      var builder = SchemaBuilder.int32().name("org.apache.kafka.connect.data.Time");
      return isOptional ? builder.optional().build() : builder.build();
    }

    // Handle basic types
    var baseSchema = convertArrowTypeToKafkaSchema(arrowType, arrowField.getChildren());

    return isOptional ? getOptionalSchema(baseSchema) : baseSchema;
  }

  /** Convert Arrow type to Kafka Connect schema */
  private org.apache.kafka.connect.data.Schema convertArrowTypeToKafkaSchema(
      ArrowType arrowType, List<Field> children) {

    if (arrowType instanceof ArrowType.Int intType) {
      return switch (intType.getBitWidth()) {
        case 8 -> org.apache.kafka.connect.data.Schema.INT8_SCHEMA;
        case 16 -> org.apache.kafka.connect.data.Schema.INT16_SCHEMA;
        case 32 -> org.apache.kafka.connect.data.Schema.INT32_SCHEMA;
        case 64 -> org.apache.kafka.connect.data.Schema.INT64_SCHEMA;
        default ->
            throw new IllegalArgumentException(
                "Unsupported int bit width: " + intType.getBitWidth());
      };
    }

    if (arrowType instanceof ArrowType.FloatingPoint floatType) {
      return switch (floatType.getPrecision()) {
        case SINGLE -> org.apache.kafka.connect.data.Schema.FLOAT32_SCHEMA;
        case DOUBLE -> org.apache.kafka.connect.data.Schema.FLOAT64_SCHEMA;
        default ->
            throw new IllegalArgumentException(
                "Unsupported float precision: " + floatType.getPrecision());
      };
    }

    if (arrowType instanceof ArrowType.Bool) {
      return org.apache.kafka.connect.data.Schema.BOOLEAN_SCHEMA;
    }

    if (arrowType instanceof ArrowType.Utf8) {
      return org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
    }

    if (arrowType instanceof ArrowType.Binary) {
      return org.apache.kafka.connect.data.Schema.BYTES_SCHEMA;
    }

    if (arrowType instanceof ArrowType.List) {
      if (children.isEmpty()) {
        throw new IllegalArgumentException("List type must have exactly one child field");
      }
      var elementSchema = convertArrowFieldToKafka(children.get(0));
      return SchemaBuilder.array(elementSchema).build();
    }

    if (arrowType instanceof ArrowType.Map) {
      if (children.size() != 1 || !"entries".equals(children.get(0).getName())) {
        throw new IllegalArgumentException(
            "Map type must have exactly one child field named 'entries'");
      }
      var entriesField = children.get(0);
      if (entriesField.getChildren().size() != 2) {
        throw new IllegalArgumentException(
            "Map entries must have exactly two children (key and value)");
      }

      var keyField = entriesField.getChildren().get(0);
      var valueField = entriesField.getChildren().get(1);

      var keySchema = convertArrowFieldToKafka(keyField);
      var valueSchema = convertArrowFieldToKafka(valueField);

      return SchemaBuilder.map(keySchema, valueSchema).build();
    }

    if (arrowType instanceof ArrowType.Struct) {
      var structBuilder = SchemaBuilder.struct();
      for (Field child : children) {
        var childSchema = convertArrowFieldToKafka(child);
        structBuilder.field(child.getName(), childSchema);
      }
      return structBuilder.build();
    }

    throw new IllegalArgumentException(
        "Unsupported Arrow type: " + arrowType.getClass().getSimpleName());
  }

  /** Get the optional version of a schema */
  private org.apache.kafka.connect.data.Schema getOptionalSchema(
      org.apache.kafka.connect.data.Schema schema) {
    // Use predefined optional schemas when available
    if (schema == org.apache.kafka.connect.data.Schema.INT8_SCHEMA) {
      return org.apache.kafka.connect.data.Schema.OPTIONAL_INT8_SCHEMA;
    }
    if (schema == org.apache.kafka.connect.data.Schema.INT16_SCHEMA) {
      return org.apache.kafka.connect.data.Schema.OPTIONAL_INT16_SCHEMA;
    }
    if (schema == org.apache.kafka.connect.data.Schema.INT32_SCHEMA) {
      return org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA;
    }
    if (schema == org.apache.kafka.connect.data.Schema.INT64_SCHEMA) {
      return org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA;
    }
    if (schema == org.apache.kafka.connect.data.Schema.FLOAT32_SCHEMA) {
      return org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT32_SCHEMA;
    }
    if (schema == org.apache.kafka.connect.data.Schema.FLOAT64_SCHEMA) {
      return org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT64_SCHEMA;
    }
    if (schema == org.apache.kafka.connect.data.Schema.BOOLEAN_SCHEMA) {
      return org.apache.kafka.connect.data.Schema.OPTIONAL_BOOLEAN_SCHEMA;
    }
    if (schema == org.apache.kafka.connect.data.Schema.STRING_SCHEMA) {
      return org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA;
    }
    if (schema == org.apache.kafka.connect.data.Schema.BYTES_SCHEMA) {
      return org.apache.kafka.connect.data.Schema.OPTIONAL_BYTES_SCHEMA;
    }

    // For complex types, handle each type specifically
    switch (schema.type()) {
      case ARRAY:
        return SchemaBuilder.array(schema.valueSchema()).optional().build();
      case MAP:
        return SchemaBuilder.map(schema.keySchema(), schema.valueSchema()).optional().build();
      case STRUCT:
        var structBuilder = SchemaBuilder.struct().optional();
        if (schema.name() != null) {
          structBuilder.name(schema.name());
        }
        if (schema.doc() != null) {
          structBuilder.doc(schema.doc());
        }
        for (var field : schema.fields()) {
          structBuilder.field(field.name(), field.schema());
        }
        return structBuilder.build();
      default:
        // For other complex types, create an optional version using SchemaBuilder
        var builder = SchemaBuilder.type(schema.type()).optional();
        if (schema.name() != null) {
          builder.name(schema.name());
        }
        if (schema.doc() != null) {
          builder.doc(schema.doc());
        }
        return builder.build();
    }
  }

  public void close() {
    try {
      allocator.close();
    } catch (Exception e) {
      LOG.warn("Failed to close allocator", e);
    }
  }
}
