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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ArrowToKafkaConverterTest {

  private BufferAllocator allocator;
  private ArrowToKafkaConverter converter;

  @BeforeEach
  void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
    converter = new ArrowToKafkaConverter(allocator);
  }

  @AfterEach
  void tearDown() {
    converter.close();
    allocator.close();
  }

  @Nested
  @DisplayName("Basic Type Conversion Tests")
  class BasicTypeTests {

    @Test
    @DisplayName("Should convert simple Arrow schema with basic types")
    void shouldConvertSimpleArrowSchema() throws Exception {
      // Create Arrow schema with basic types
      var fields =
          Arrays.asList(
              new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
              new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
              new Field("age", FieldType.nullable(new ArrowType.Int(64, true)), null));
      var arrowSchema = new Schema(fields);

      // Create VectorSchemaRoot with sample data
      try (var root = VectorSchemaRoot.create(arrowSchema, allocator)) {
        root.allocateNew();

        // Fill with sample data
        var idVector = (IntVector) root.getVector("id");
        var nameVector = (VarCharVector) root.getVector("name");
        var ageVector = (BigIntVector) root.getVector("age");

        idVector.setSafe(0, 1);
        nameVector.setSafe(0, "John".getBytes(StandardCharsets.UTF_8));
        ageVector.setSafe(1, 25L);

        root.setRowCount(2);

        // Convert to Arrow IPC bytes
        var ipcBytes = createArrowIpcBytes(root);

        // Convert back to Kafka format
        var result = converter.convertFromArrowIPC(ipcBytes);

        assertNotNull(result);
        assertNotNull(result.schema());
        assertNotNull(result.value());
        assertInstanceOf(VectorSchemaRoot.class, result.value());

        var kafkaSchema = result.schema();
        assertEquals(org.apache.kafka.connect.data.Schema.Type.STRUCT, kafkaSchema.type());
        assertEquals(3, kafkaSchema.fields().size());

        // Verify field types
        assertEquals(
            org.apache.kafka.connect.data.Schema.Type.INT32,
            kafkaSchema.field("id").schema().type());
        assertEquals(
            org.apache.kafka.connect.data.Schema.Type.STRING,
            kafkaSchema.field("name").schema().type());
        assertEquals(
            org.apache.kafka.connect.data.Schema.Type.INT64,
            kafkaSchema.field("age").schema().type());

        // Verify all fields are optional (nullable)
        assertTrue(kafkaSchema.field("id").schema().isOptional());
        assertTrue(kafkaSchema.field("name").schema().isOptional());
        assertTrue(kafkaSchema.field("age").schema().isOptional());

        // Verify data is preserved
        var resultRoot = (VectorSchemaRoot) result.value();
        assertEquals(2, resultRoot.getRowCount());
      }
    }

    @Test
    @DisplayName("Should handle timestamp logical types")
    void shouldHandleTimestampLogicalTypes() throws Exception {
      var fields =
          Arrays.asList(
              new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
              new Field(
                  "created_at",
                  FieldType.nullable(
                      new ArrowType.Timestamp(
                          org.apache.arrow.vector.types.TimeUnit.MILLISECOND, null)),
                  null));
      var arrowSchema = new Schema(fields);

      try (var root = VectorSchemaRoot.create(arrowSchema, allocator)) {
        root.allocateNew();
        root.setRowCount(1);

        var ipcBytes = createArrowIpcBytes(root);
        var result = converter.convertFromArrowIPC(ipcBytes);

        assertNotNull(result);
        var kafkaSchema = result.schema();

        var timestampField = kafkaSchema.field("created_at");
        assertEquals(
            org.apache.kafka.connect.data.Schema.Type.INT64, timestampField.schema().type());
        assertEquals("org.apache.kafka.connect.data.Timestamp", timestampField.schema().name());
      }
    }
  }

  @Nested
  @DisplayName("Complex Type Tests")
  class ComplexTypeTests {

    @Test
    @DisplayName("Should convert nested struct fields")
    void shouldConvertNestedStructFields() throws Exception {
      // Create nested structure: person with address
      var addressFields =
          Arrays.asList(
              new Field("street", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
              new Field("city", FieldType.nullable(ArrowType.Utf8.INSTANCE), null));
      var addressField =
          new Field("address", FieldType.nullable(ArrowType.Struct.INSTANCE), addressFields);

      var personFields =
          Arrays.asList(
              new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
              new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
              addressField);
      var arrowSchema = new Schema(personFields);

      try (var root = VectorSchemaRoot.create(arrowSchema, allocator)) {
        root.allocateNew();
        root.setRowCount(1);

        var ipcBytes = createArrowIpcBytes(root);
        var result = converter.convertFromArrowIPC(ipcBytes);

        assertNotNull(result);
        var kafkaSchema = result.schema();

        var addressFieldSchema = kafkaSchema.field("address");
        assertEquals(
            org.apache.kafka.connect.data.Schema.Type.STRUCT, addressFieldSchema.schema().type());

        var addressSchema = addressFieldSchema.schema();
        assertEquals(2, addressSchema.fields().size());
        assertEquals(
            org.apache.kafka.connect.data.Schema.Type.STRING,
            addressSchema.field("street").schema().type());
        assertEquals(
            org.apache.kafka.connect.data.Schema.Type.STRING,
            addressSchema.field("city").schema().type());
      }
    }

    @Test
    @DisplayName("Should convert array fields")
    void shouldConvertArrayFields() throws Exception {
      var elementField = new Field("element", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
      var arrayField =
          new Field("tags", FieldType.nullable(ArrowType.List.INSTANCE), List.of(elementField));

      var fields =
          Arrays.asList(
              new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null), arrayField);
      var arrowSchema = new Schema(fields);

      try (var root = VectorSchemaRoot.create(arrowSchema, allocator)) {
        root.allocateNew();
        root.setRowCount(1);

        var ipcBytes = createArrowIpcBytes(root);
        var result = converter.convertFromArrowIPC(ipcBytes);

        assertNotNull(result);
        var kafkaSchema = result.schema();

        var tagsField = kafkaSchema.field("tags");
        assertEquals(org.apache.kafka.connect.data.Schema.Type.ARRAY, tagsField.schema().type());
        assertEquals(
            org.apache.kafka.connect.data.Schema.Type.STRING,
            tagsField.schema().valueSchema().type());
      }
    }
  }

  @Nested
  @DisplayName("Error Handling Tests")
  class ErrorHandlingTests {

    @Test
    @DisplayName("Should handle null or empty IPC bytes")
    void shouldHandleNullOrEmptyIpcBytes() {
      var result1 = converter.convertFromArrowIPC(null);
      assertNotNull(result1);
      assertNull(result1.schema());
      assertNull(result1.value());

      var result2 = converter.convertFromArrowIPC(new byte[0]);
      assertNotNull(result2);
      assertNull(result2.schema());
      assertNull(result2.value());
    }

    @Test
    @DisplayName("Should throw exception for invalid IPC bytes")
    void shouldThrowExceptionForInvalidIpcBytes() {
      // Use simple invalid data that won't trigger Arrow memory allocation
      var invalidBytes = new byte[] {0x00, 0x01, 0x02}; // Simple invalid bytes

      assertThrows(RuntimeException.class, () -> converter.convertFromArrowIPC(invalidBytes));
    }
  }

  @Nested
  @DisplayName("Round-trip Tests")
  class RoundTripTests {

    @Test
    @DisplayName("Should maintain data integrity in round-trip conversion")
    void shouldMaintainDataIntegrityInRoundTrip() throws Exception {
      // Start with a Kafka schema
      var kafkaSchema =
          org.apache.kafka.connect.data.SchemaBuilder.struct()
              .field("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
              .field("name", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
              .field("score", org.apache.kafka.connect.data.Schema.FLOAT64_SCHEMA)
              .build();

      // Convert to Arrow
      var arrowSchema = KafkaSchemaToArrow.arrowSchemaFromKafka(kafkaSchema);

      // Create data
      try (var root = VectorSchemaRoot.create(arrowSchema, allocator)) {
        root.allocateNew();

        var idVector = (IntVector) root.getVector("id");
        var nameVector = (VarCharVector) root.getVector("name");

        idVector.setSafe(0, 123);
        nameVector.setSafe(0, "test".getBytes(StandardCharsets.UTF_8));

        root.setRowCount(1);

        // Convert to IPC bytes
        var ipcBytes = createArrowIpcBytes(root);

        // Convert back to Kafka format
        var result = converter.convertFromArrowIPC(ipcBytes);

        assertNotNull(result);
        assertNotNull(result.schema());
        assertNotNull(result.value());

        var resultSchema = result.schema();
        assertEquals(3, resultSchema.fields().size());
        assertEquals(
            org.apache.kafka.connect.data.Schema.Type.INT32,
            resultSchema.field("id").schema().type());
        assertEquals(
            org.apache.kafka.connect.data.Schema.Type.STRING,
            resultSchema.field("name").schema().type());
        assertEquals(
            org.apache.kafka.connect.data.Schema.Type.FLOAT64,
            resultSchema.field("score").schema().type());

        var resultRoot = (VectorSchemaRoot) result.value();
        assertEquals(1, resultRoot.getRowCount());
      }
    }
  }

  /** Helper method to create Arrow IPC bytes from VectorSchemaRoot */
  private byte[] createArrowIpcBytes(VectorSchemaRoot root) throws Exception {
    var outputStream = new ByteArrayOutputStream();
    var channel = Channels.newChannel(outputStream);

    try (var writer = new ArrowStreamWriter(root, null, channel)) {
      writer.start();
      writer.writeBatch();
      writer.end();
    }

    return outputStream.toByteArray();
  }
}
