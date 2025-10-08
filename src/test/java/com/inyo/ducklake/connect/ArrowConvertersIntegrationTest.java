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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ArrowConvertersIntegrationTest {

  private BufferAllocator allocator;
  private ArrowIpcConverter converter;

  @BeforeEach
  void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
    converter = new ArrowIpcConverter();

    Map<String, Object> configs = new HashMap<>();
    converter.configure(configs, false);
  }

  @AfterEach
  void tearDown() {
    converter.close();
    allocator.close();
  }

  @Test
  void shouldCompleteFullRoundTripWithKafkaSchemaIntegration() {
    // 1. Create a Kafka Connect schema
    var kafkaSchema =
        SchemaBuilder.struct()
            .field("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
            .field("name", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
            .field("active", org.apache.kafka.connect.data.Schema.BOOLEAN_SCHEMA)
            .build();

    // 2. Convert Kafka schema to Arrow schema using existing converter
    var arrowSchema = KafkaSchemaToArrow.arrowSchemaFromKafka(kafkaSchema);

    // 3. Create VectorSchemaRoot with test data
    try (var root = VectorSchemaRoot.create(arrowSchema, allocator)) {
      root.allocateNew();

      var idVector = (IntVector) root.getVector("id");
      var nameVector = (VarCharVector) root.getVector("name");

      // Add test data
      idVector.setSafe(0, 100);
      idVector.setSafe(1, 200);
      nameVector.setSafe(0, "Alice".getBytes(StandardCharsets.UTF_8));
      nameVector.setSafe(1, "Bob".getBytes(StandardCharsets.UTF_8));
      // Active vector will be handled automatically for boolean values

      root.setRowCount(2);

      // 4. Serialize VectorSchemaRoot to Arrow IPC bytes using our converter
      byte[] arrowIpcBytes = converter.fromConnectData("test-topic", null, root);
      assertNotNull(arrowIpcBytes);
      assertTrue(arrowIpcBytes.length > 0);

      // 5. Deserialize back to SchemaAndValue
      var result = converter.toConnectData("test-topic", arrowIpcBytes);
      assertNotNull(result);
      assertNotNull(result.schema());
      assertNotNull(result.value());
      assertInstanceOf(VectorSchemaRoot.class, result.value());

      // 6. Verify the schema was correctly reconstructed
      var reconstructedSchema = result.schema();
      assertEquals(org.apache.kafka.connect.data.Schema.Type.STRUCT, reconstructedSchema.type());
      assertEquals(3, reconstructedSchema.fields().size());

      // Verify field types
      assertEquals(
          org.apache.kafka.connect.data.Schema.Type.INT32,
          reconstructedSchema.field("id").schema().type());
      assertEquals(
          org.apache.kafka.connect.data.Schema.Type.STRING,
          reconstructedSchema.field("name").schema().type());
      assertEquals(
          org.apache.kafka.connect.data.Schema.Type.BOOLEAN,
          reconstructedSchema.field("active").schema().type());

      // 7. Verify the data was preserved
      var reconstructedRoot = (VectorSchemaRoot) result.value();
      assertEquals(2, reconstructedRoot.getRowCount());
      assertEquals(3, reconstructedRoot.getFieldVectors().size());

      System.out.println("✅ Full round-trip test completed successfully!");
      System.out.println("Schema fields: " + reconstructedSchema.fields().size());
      System.out.println("Data rows: " + reconstructedRoot.getRowCount());
    }
  }

  @Test
  void shouldHandleTimestampFields() {
    // Create Kafka schema with timestamp
    var kafkaSchema =
        SchemaBuilder.struct()
            .field("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
            .field("created_at", org.apache.kafka.connect.data.Timestamp.SCHEMA)
            .build();

    // Convert to Arrow and back
    var arrowSchema = KafkaSchemaToArrow.arrowSchemaFromKafka(kafkaSchema);

    try (var root = VectorSchemaRoot.create(arrowSchema, allocator)) {
      root.allocateNew();
      root.setRowCount(1);

      // Serialize and deserialize
      byte[] bytes = converter.fromConnectData("test-topic", null, root);
      var result = converter.toConnectData("test-topic", bytes);

      // Verify timestamp field
      var schema = result.schema();
      var timestampField = schema.field("created_at");
      assertNotNull(timestampField);
      assertEquals(org.apache.kafka.connect.data.Schema.Type.INT64, timestampField.schema().type());
      assertEquals("org.apache.kafka.connect.data.Timestamp", timestampField.schema().name());

      System.out.println("✅ Timestamp handling test completed successfully!");
    }
  }

  @Test
  void shouldHandleNestedStructures() {
    // Create nested structure
    var addressSchema =
        SchemaBuilder.struct()
            .field("street", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
            .field("city", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
            .build();

    var personSchema =
        SchemaBuilder.struct()
            .field("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
            .field("name", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
            .field("address", addressSchema)
            .build();

    var arrowSchema = KafkaSchemaToArrow.arrowSchemaFromKafka(personSchema);

    try (var root = VectorSchemaRoot.create(arrowSchema, allocator)) {
      root.allocateNew();
      root.setRowCount(1);

      byte[] bytes = converter.fromConnectData("test-topic", null, root);
      var result = converter.toConnectData("test-topic", bytes);

      var schema = result.schema();
      var addressField = schema.field("address");
      assertNotNull(addressField);
      assertEquals(org.apache.kafka.connect.data.Schema.Type.STRUCT, addressField.schema().type());

      var addressFieldSchema = addressField.schema();
      assertEquals(2, addressFieldSchema.fields().size());
      assertNotNull(addressFieldSchema.field("street"));
      assertNotNull(addressFieldSchema.field("city"));

      System.out.println("✅ Nested structures test completed successfully!");
    }
  }

  @Test
  void shouldHandleArrayFields() throws Exception {
    var kafkaSchema =
        SchemaBuilder.struct()
            .field("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
            .field(
                "tags",
                SchemaBuilder.array(org.apache.kafka.connect.data.Schema.STRING_SCHEMA).build())
            .build();

    var arrowSchema = KafkaSchemaToArrow.arrowSchemaFromKafka(kafkaSchema);

    try (var root = VectorSchemaRoot.create(arrowSchema, allocator)) {
      root.allocateNew();
      root.setRowCount(1);

      byte[] bytes = converter.fromConnectData("test-topic", null, root);
      var result = converter.toConnectData("test-topic", bytes);

      var schema = result.schema();
      var tagsField = schema.field("tags");
      assertNotNull(tagsField);
      assertEquals(org.apache.kafka.connect.data.Schema.Type.ARRAY, tagsField.schema().type());
      assertEquals(
          org.apache.kafka.connect.data.Schema.Type.STRING,
          tagsField.schema().valueSchema().type());

      System.out.println("✅ Array fields test completed successfully!");
    }
  }
}
