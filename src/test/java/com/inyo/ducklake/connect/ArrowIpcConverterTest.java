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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ArrowIpcConverterTest {

  private BufferAllocator allocator;
  private ArrowIpcConverter converter;
  private static final String TEST_TOPIC = "test-topic";

  @BeforeEach
  void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
    converter = new ArrowIpcConverter();

    // Configure the converter
    Map<String, Object> configs = new HashMap<>();
    converter.configure(configs, false); // Configure as value converter
  }

  @AfterEach
  void tearDown() {
    converter.close();
    allocator.close();
  }

  @Nested
  @DisplayName("Configuration Tests")
  class ConfigurationTests {

    @Test
    @DisplayName("Should configure converter for key")
    void shouldConfigureConverterForKey() {
      var keyConverter = new ArrowIpcConverter();
      Map<String, Object> configs = new HashMap<>();

      // Should not throw exception
      assertDoesNotThrow(() -> keyConverter.configure(configs, true));

      keyConverter.close();
    }

    @Test
    @DisplayName("Should configure converter for value")
    void shouldConfigureConverterForValue() {
      var valueConverter = new ArrowIpcConverter();
      Map<String, Object> configs = new HashMap<>();
      configs.put("some.config", "some.value");

      // Should not throw exception
      assertDoesNotThrow(() -> valueConverter.configure(configs, false));

      valueConverter.close();
    }
  }

  @Nested
  @DisplayName("Serialization Tests")
  class SerializationTests {

    @Test
    @DisplayName("Should serialize VectorSchemaRoot to Arrow IPC bytes")
    void shouldSerializeVectorSchemaRootToArrowIpcBytes() {
      // Create Arrow schema
      var fields =
          Arrays.asList(
              new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
              new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null));
      var schema = new Schema(fields);

      // Create VectorSchemaRoot with data
      try (var root = VectorSchemaRoot.create(schema, allocator)) {
        root.allocateNew();

        var idVector = (IntVector) root.getVector("id");
        var nameVector = (VarCharVector) root.getVector("name");

        idVector.setSafe(0, 123);
        nameVector.setSafe(0, "test".getBytes(StandardCharsets.UTF_8));
        root.setRowCount(1);

        // Serialize to bytes
        byte[] result = converter.fromConnectData(TEST_TOPIC, null, root);

        assertNotNull(result);
        assertTrue(result.length > 0);
      }
    }

    @Test
    @DisplayName("Should handle null value in serialization")
    void shouldHandleNullValueInSerialization() {
      byte[] result = converter.fromConnectData(TEST_TOPIC, null, null);
      assertNull(result);
    }

    @Test
    @DisplayName("Should throw DataException for invalid object type")
    void shouldThrowDataExceptionForInvalidObjectType() {
      String invalidValue = "not a VectorSchemaRoot";

      assertThrows(
          DataException.class, () -> converter.fromConnectData(TEST_TOPIC, null, invalidValue));
    }

    @Test
    @DisplayName("Should serialize empty VectorSchemaRoot")
    void shouldSerializeEmptyVectorSchemaRoot() {
      var fields = List.of(new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null));
      var schema = new Schema(fields);

      try (var root = VectorSchemaRoot.create(schema, allocator)) {
        root.allocateNew();
        root.setRowCount(0); // Empty root

        byte[] result = converter.fromConnectData(TEST_TOPIC, null, root);

        assertNotNull(result);
        assertTrue(result.length > 0);
      }
    }
  }

  @Nested
  @DisplayName("Deserialization Tests")
  class DeserializationTests {

    @Test
    @DisplayName("Should deserialize Arrow IPC bytes to SchemaAndValue")
    void shouldDeserializeArrowIpcBytesToSchemaAndValue() {
      // First create some Arrow IPC data
      var fields =
          Arrays.asList(
              new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
              new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null));
      var schema = new Schema(fields);

      byte[] arrowIpcBytes;
      try (var root = VectorSchemaRoot.create(schema, allocator)) {
        root.allocateNew();

        var idVector = (IntVector) root.getVector("id");
        var nameVector = (VarCharVector) root.getVector("name");

        idVector.setSafe(0, 456);
        nameVector.setSafe(0, "test-name".getBytes(StandardCharsets.UTF_8));
        root.setRowCount(1);

        // Serialize to get IPC bytes
        arrowIpcBytes = converter.fromConnectData(TEST_TOPIC, null, root);
      }

      // Now deserialize
      SchemaAndValue result = converter.toConnectData(TEST_TOPIC, arrowIpcBytes);

      assertNotNull(result);
      assertNotNull(result.schema());
      assertNotNull(result.value());
      assertInstanceOf(VectorSchemaRoot.class, result.value());

      var kafkaSchema = result.schema();
      assertEquals(org.apache.kafka.connect.data.Schema.Type.STRUCT, kafkaSchema.type());
      assertEquals(2, kafkaSchema.fields().size());

      var resultRoot = (VectorSchemaRoot) result.value();
      assertEquals(1, resultRoot.getRowCount());
    }

    @Test
    @DisplayName("Should handle null bytes in deserialization")
    void shouldHandleNullBytesInDeserialization() {
      SchemaAndValue result = converter.toConnectData(TEST_TOPIC, null);

      assertNotNull(result);
      assertNull(result.schema());
      assertNull(result.value());
    }

    @Test
    @DisplayName("Should handle empty bytes in deserialization")
    void shouldHandleEmptyBytesInDeserialization() {
      SchemaAndValue result = converter.toConnectData(TEST_TOPIC, new byte[0]);

      assertNotNull(result);
      assertNull(result.schema());
      assertNull(result.value());
    }

    @Test
    @DisplayName("Should throw DataException for invalid Arrow IPC bytes")
    void shouldThrowDataExceptionForInvalidArrowIpcBytes() {
      // Use very simple invalid data that won't trigger memory allocation
      byte[] invalidBytes = new byte[] {0x01, 0x02, 0x03}; // Simple invalid bytes

      assertThrows(DataException.class, () -> converter.toConnectData(TEST_TOPIC, invalidBytes));
    }
  }

  @Nested
  @DisplayName("Round-trip Tests")
  class RoundTripTests {

    @Test
    @DisplayName("Should maintain data integrity in round-trip conversion")
    void shouldMaintainDataIntegrityInRoundTrip() {
      // Create original data
      var fields =
          Arrays.asList(
              new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
              new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
              new Field("active", FieldType.nullable(ArrowType.Bool.INSTANCE), null));
      var schema = new Schema(fields);

      try (var root = VectorSchemaRoot.create(schema, allocator)) {
        root.allocateNew();

        var idVector = (IntVector) root.getVector("id");
        var nameVector = (VarCharVector) root.getVector("name");

        idVector.setSafe(0, 789);
        idVector.setSafe(1, 101112);
        nameVector.setSafe(0, "first".getBytes(StandardCharsets.UTF_8));
        nameVector.setSafe(1, "second".getBytes(StandardCharsets.UTF_8));
        root.setRowCount(2);

        // Serialize to bytes
        byte[] serialized = converter.fromConnectData(TEST_TOPIC, null, root);
        assertNotNull(serialized);

        // Deserialize back
        SchemaAndValue deserialized = converter.toConnectData(TEST_TOPIC, serialized);
        assertNotNull(deserialized);
        assertNotNull(deserialized.value());
        assertInstanceOf(VectorSchemaRoot.class, deserialized.value());

        // Verify data integrity
        var deserializedRoot = (VectorSchemaRoot) deserialized.value();
        assertEquals(2, deserializedRoot.getRowCount());
        assertEquals(3, deserializedRoot.getFieldVectors().size());

        // Verify schema structure
        var kafkaSchema = deserialized.schema();
        assertEquals(3, kafkaSchema.fields().size());
        assertEquals(
            org.apache.kafka.connect.data.Schema.Type.INT32,
            kafkaSchema.field("id").schema().type());
        assertEquals(
            org.apache.kafka.connect.data.Schema.Type.STRING,
            kafkaSchema.field("name").schema().type());
        assertEquals(
            org.apache.kafka.connect.data.Schema.Type.BOOLEAN,
            kafkaSchema.field("active").schema().type());
      }
    }

    @Test
    @DisplayName("Should handle multiple round-trips consistently")
    void shouldHandleMultipleRoundTripsConsistently() {
      var fields =
          List.of(new Field("value", FieldType.nullable(new ArrowType.Int(64, true)), null));
      var schema = new Schema(fields);

      byte[] currentBytes;
      try (var root = VectorSchemaRoot.create(schema, allocator)) {
        root.allocateNew();
        root.setRowCount(0); // Empty data
        currentBytes = converter.fromConnectData(TEST_TOPIC, null, root);
      }

      // Perform multiple round-trips
      for (int i = 0; i < 3; i++) {
        // Deserialize
        SchemaAndValue deserialized = converter.toConnectData(TEST_TOPIC, currentBytes);
        assertNotNull(deserialized);

        // Serialize again
        currentBytes = converter.fromConnectData(TEST_TOPIC, null, deserialized.value());
        assertNotNull(currentBytes);
      }

      // Final verification
      SchemaAndValue finalResult = converter.toConnectData(TEST_TOPIC, currentBytes);
      assertNotNull(finalResult);
      assertNotNull(finalResult.schema());
      assertNotNull(finalResult.value());
    }
  }

  @Nested
  @DisplayName("Resource Management Tests")
  class ResourceManagementTests {

    @Test
    @DisplayName("Should close resources without throwing exceptions")
    void shouldCloseResourcesWithoutThrowingExceptions() {
      var testConverter = new ArrowIpcConverter();
      Map<String, Object> configs = new HashMap<>();
      testConverter.configure(configs, false);

      // Should not throw exception
      assertDoesNotThrow(testConverter::close);
    }

    @Test
    @DisplayName("Should handle multiple close calls gracefully")
    void shouldHandleMultipleCloseCallsGracefully() {
      var testConverter = new ArrowIpcConverter();
      Map<String, Object> configs = new HashMap<>();
      testConverter.configure(configs, false);

      // Multiple close calls should not throw exceptions
      assertDoesNotThrow(
          () -> {
            testConverter.close();
            testConverter.close();
            testConverter.close();
          });
    }
  }
}
