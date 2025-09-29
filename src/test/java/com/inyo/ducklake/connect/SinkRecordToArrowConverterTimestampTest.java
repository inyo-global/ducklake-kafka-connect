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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("SinkRecordToArrowConverter Timestamp Support Tests")
class SinkRecordToArrowConverterTimestampTest {

  private BufferAllocator allocator;
  private SinkRecordToArrowConverter converter;

  @BeforeEach
  void setUp() {
    allocator = new RootAllocator();
    converter = new SinkRecordToArrowConverter(allocator);
  }

  @AfterEach
  void tearDown() {
    if (converter != null) {
      converter.close();
    }
    if (allocator != null) {
      allocator.close();
    }
  }

  @Nested
  @DisplayName("Automatic Timestamp Detection Tests")
  class AutomaticTimestampDetectionTests {

    @Test
    @DisplayName("Should detect timestamps in JSON data without schema")
    void shouldDetectTimestampsInJSONData() {
      // Create a record with JSON string containing timestamps
      String jsonValue =
          "{\"id\": 123, \"created_at\": \"2025-09-25T18:05:12Z\", \"name\": \"test\"}";

      SinkRecord record = new SinkRecord("test-topic", 0, null, null, null, jsonValue, 0L);

      try (VectorSchemaRoot result = converter.convertRecords(List.of(record))) {
        // Verify schema was inferred correctly
        org.apache.arrow.vector.types.pojo.Schema schema = result.getSchema();
        assertNotNull(schema);

        Field createdAtField = schema.findField("created_at");
        assertNotNull(createdAtField);
        assertInstanceOf(
            ArrowType.Timestamp.class,
            createdAtField.getType(),
            "created_at field should be detected as timestamp");

        Field idField = schema.findField("id");
        Field nameField = schema.findField("name");
        assertNotNull(idField);
        assertNotNull(nameField);

        // Verify data was populated correctly
        assertEquals(1, result.getRowCount());

        TimeStampMilliVector timestampVector =
            (TimeStampMilliVector) result.getVector("created_at");
        assertNotNull(timestampVector);
        assertFalse(timestampVector.isNull(0));

        // Expected timestamp: 2025-09-25T18:05:12Z
        long expectedMillis = TimestampUtils.parseTimestampToEpochMillis("2025-09-25T18:05:12Z");
        assertEquals(expectedMillis, timestampVector.get(0));
      }
    }

    @Test
    @DisplayName("Should detect timestamps in Map data without schema")
    void shouldDetectTimestampsInMapData() {
      Map<String, Object> mapValue = new HashMap<>();
      mapValue.put("id", 456);
      mapValue.put("timestamp", "2025-09-25T18:05:12");
      mapValue.put("updated_at", "2025-09-25T20:30:45.123Z");
      mapValue.put("description", "regular string");

      SinkRecord record = new SinkRecord("test-topic", 0, null, null, null, mapValue, 0L);

      try (VectorSchemaRoot result = converter.convertRecords(List.of(record))) {
        org.apache.arrow.vector.types.pojo.Schema schema = result.getSchema();

        Field timestampField = schema.findField("timestamp");
        Field updatedAtField = schema.findField("updated_at");
        Field descriptionField = schema.findField("description");

        assertNotNull(timestampField);
        assertNotNull(updatedAtField);
        assertNotNull(descriptionField);

        // Timestamp fields should be detected as timestamps
        assertInstanceOf(ArrowType.Timestamp.class, timestampField.getType());
        assertInstanceOf(ArrowType.Timestamp.class, updatedAtField.getType());

        // Regular string should remain as string
        assertInstanceOf(ArrowType.Utf8.class, descriptionField.getType());

        // Verify data conversion
        TimeStampMilliVector timestampVector = (TimeStampMilliVector) result.getVector("timestamp");
        TimeStampMilliVector updatedAtVector =
            (TimeStampMilliVector) result.getVector("updated_at");

        long expectedTimestamp = TimestampUtils.parseTimestampToEpochMillis("2025-09-25T18:05:12");
        long expectedUpdatedAt =
            TimestampUtils.parseTimestampToEpochMillis("2025-09-25T20:30:45.123Z");

        assertEquals(expectedTimestamp, timestampVector.get(0));
        assertEquals(expectedUpdatedAt, updatedAtVector.get(0));
      }
    }

    @Test
    @DisplayName("Should handle mixed timestamp formats")
    void shouldHandleMixedTimestampFormats() {
      SinkRecord record = getSinkRecord();

      try (VectorSchemaRoot result = converter.convertRecords(List.of(record))) {
        org.apache.arrow.vector.types.pojo.Schema schema = result.getSchema();

        // First 4 should be timestamps
        for (int i = 1; i <= 4; i++) {
          Field field = schema.findField("ts" + i);
          assertNotNull(field);
          assertInstanceOf(
              ArrowType.Timestamp.class,
              field.getType(),
              "ts" + i + " should be detected as timestamp");
        }

        // Last one should be string
        Field notTimestampField = schema.findField("not_timestamp");
        assertNotNull(notTimestampField);
        assertInstanceOf(
            ArrowType.Utf8.class,
            notTimestampField.getType(),
            "not_timestamp should remain as string");
      }
    }

    @NotNull
    private static SinkRecord getSinkRecord() {
      Map<String, Object> mapValue = new HashMap<>();
      mapValue.put("ts1", "2025-09-25T18:05:12Z"); // with Z timezone
      mapValue.put("ts2", "2025-09-25T18:05:12+03:00"); // with offset
      mapValue.put("ts3", "2025-09-25T18:05:12"); // without timezone
      mapValue.put("ts4", "2025-09-25T18:05:12.123456"); // with microseconds
      mapValue.put("not_timestamp", "2025-09-25 18:05:12"); // different format, should be string

      return new SinkRecord("test-topic", 0, null, null, null, mapValue, 0L);
    }
  }

  @Nested
  @DisplayName("Structured Data with Timestamps Tests")
  class StructuredDataWithTimestampsTests {

    @Test
    @DisplayName("Should handle records with explicit timestamp schema")
    void shouldHandleExplicitTimestampSchema() {
      Schema schema =
          SchemaBuilder.struct()
              .field("id", Schema.INT64_SCHEMA)
              .field("created_at", Timestamp.SCHEMA)
              .field("name", Schema.STRING_SCHEMA)
              .build();

      Struct struct = new Struct(schema);
      struct.put("id", 123L);
      struct.put("created_at", new java.util.Date(1695748512000L)); // timestamp as Date
      struct.put("name", "test record");

      SinkRecord record = new SinkRecord("test-topic", 0, null, null, schema, struct, 0L);

      try (VectorSchemaRoot result = converter.convertRecords(List.of(record))) {
        Field createdAtField = result.getSchema().findField("created_at");
        assertNotNull(createdAtField);
        assertInstanceOf(ArrowType.Timestamp.class, createdAtField.getType());

        TimeStampMilliVector timestampVector =
            (TimeStampMilliVector) result.getVector("created_at");
        assertEquals(1695748512000L, timestampVector.get(0));
      }
    }

    @Test
    @DisplayName("Should handle timestamp strings in structured data")
    void shouldHandleTimestampStringsInStructuredData() {
      Schema schema =
          SchemaBuilder.struct()
              .field("id", Schema.INT64_SCHEMA)
              .field("created_at", Timestamp.SCHEMA)
              .field("name", Schema.STRING_SCHEMA)
              .build();

      Struct struct = new Struct(schema);
      struct.put("id", 123L);
      struct.put(
          "created_at",
          new java.util.Date(TimestampUtils.parseTimestampToEpochMillis("2025-09-25T18:05:12Z")));
      struct.put("name", "test record");

      SinkRecord record = new SinkRecord("test-topic", 0, null, null, schema, struct, 0L);

      try (VectorSchemaRoot result = converter.convertRecords(List.of(record))) {
        TimeStampMilliVector timestampVector =
            (TimeStampMilliVector) result.getVector("created_at");
        long expectedMillis = TimestampUtils.parseTimestampToEpochMillis("2025-09-25T18:05:12Z");
        assertEquals(expectedMillis, timestampVector.get(0));
      }
    }

    @Test
    @DisplayName("Should handle nested structures with timestamps")
    void shouldHandleNestedStructuresWithTimestamps() {
      SinkRecord record = getSinkRecord();

      try (VectorSchemaRoot result = converter.convertRecords(List.of(record))) {
        // Check root level timestamp
        Field rootTimestampField = result.getSchema().findField("created_at");
        assertNotNull(rootTimestampField);
        assertInstanceOf(ArrowType.Timestamp.class, rootTimestampField.getType());

        // Check nested structure exists
        Field addressField = result.getSchema().findField("address");
        assertNotNull(addressField);
        assertInstanceOf(ArrowType.Struct.class, addressField.getType());

        // Check nested timestamp field
        Field nestedTimestampField = null;
        for (Field child : addressField.getChildren()) {
          if ("created_at".equals(child.getName())) {
            nestedTimestampField = child;
            break;
          }
        }
        assertNotNull(nestedTimestampField);
        assertInstanceOf(ArrowType.Timestamp.class, nestedTimestampField.getType());
      }
    }

    @NotNull
    private static SinkRecord getSinkRecord() {
      Map<String, Object> nestedObject = new HashMap<>();
      nestedObject.put("street", "123 Main St");
      nestedObject.put("created_at", "2025-09-25T18:05:12Z");

      Map<String, Object> rootObject = new HashMap<>();
      rootObject.put("id", 123);
      rootObject.put("name", "John Doe");
      rootObject.put("created_at", "2025-09-25T17:00:00Z");
      rootObject.put("address", nestedObject);

      return new SinkRecord("test-topic", 0, null, null, null, rootObject, 0L);
    }
  }

  @Nested
  @DisplayName("Error Handling Tests")
  class ErrorHandlingTests {

    @Test
    @DisplayName("Should handle invalid timestamp strings gracefully")
    void shouldHandleInvalidTimestampStringsGracefully() {
      Map<String, Object> mapValue = new HashMap<>();
      mapValue.put("valid_timestamp", "2025-09-25T18:05:12Z");
      mapValue.put("invalid_timestamp", "not-a-timestamp");
      mapValue.put("almost_timestamp", "2025-13-45T25:70:80"); // invalid date/time

      SinkRecord record = new SinkRecord("test-topic", 0, null, null, null, mapValue, 0L);

      // Should not throw exception, but handle gracefully

      try (VectorSchemaRoot result = converter.convertRecords(List.of(record))) {
        Field validField = result.getSchema().findField("valid_timestamp");
        Field invalidField = result.getSchema().findField("invalid_timestamp");
        Field almostField = result.getSchema().findField("almost_timestamp");

        // Valid timestamp should be detected as timestamp
        assertInstanceOf(ArrowType.Timestamp.class, validField.getType());

        // Invalid ones should be treated as strings
        assertInstanceOf(ArrowType.Utf8.class, invalidField.getType());
        assertInstanceOf(ArrowType.Utf8.class, almostField.getType());
      }
    }

    @Test
    @DisplayName("Should handle null timestamp values")
    void shouldHandleNullTimestampValues() {
      Map<String, Object> mapValue = new HashMap<>();
      mapValue.put("id", 123);
      mapValue.put("created_at", "2025-09-25T18:05:12Z");
      mapValue.put("updated_at", null);

      SinkRecord record = new SinkRecord("test-topic", 0, null, null, null, mapValue, 0L);

      // Should handle records where some timestamp values are null
      assertDoesNotThrow(
          () -> {
            VectorSchemaRoot result = converter.convertRecords(List.of(record));
            result.close();
          });
    }

    @Test
    @DisplayName("Should handle empty records")
    void shouldHandleEmptyRecords() {
      Map<String, Object> emptyMap = new HashMap<>();

      SinkRecord record = new SinkRecord("test-topic", 0, null, null, null, emptyMap, 0L);

      // Should handle empty records gracefully
      assertDoesNotThrow(
          () -> {
            VectorSchemaRoot result = converter.convertRecords(List.of(record));
            result.close();
          });
    }
  }

  @Nested
  @DisplayName("Multiple Records Tests")
  class MultipleRecordsTests {

    @Test
    @DisplayName("Should handle multiple records with consistent timestamp fields")
    void shouldHandleMultipleRecordsWithConsistentTimestamps() {
      List<SinkRecord> records = new ArrayList<>();

      for (int i = 0; i < 3; i++) {
        Map<String, Object> mapValue = new HashMap<>();
        mapValue.put("id", i + 1);
        mapValue.put("created_at", "2025-09-25T" + String.format("%02d", 8 + i) + ":05:12Z");
        mapValue.put("name", "Record " + (i + 1));

        records.add(new SinkRecord("test-topic", 0, null, null, null, mapValue, i));
      }

      try (VectorSchemaRoot result = converter.convertRecords(records)) {
        assertEquals(3, result.getRowCount());

        Field createdAtField = result.getSchema().findField("created_at");
        assertInstanceOf(ArrowType.Timestamp.class, createdAtField.getType());

        TimeStampMilliVector timestampVector =
            (TimeStampMilliVector) result.getVector("created_at");

        // Verify all timestamps were converted correctly
        for (int i = 0; i < 3; i++) {
          assertFalse(timestampVector.isNull(i));
          long expected =
              TimestampUtils.parseTimestampToEpochMillis(
                  "2025-09-25T" + String.format("%02d", 8 + i) + ":05:12Z");
          assertEquals(expected, timestampVector.get(i));
        }
      }
    }

    @Test
    @DisplayName("Should handle schema evolution with timestamps")
    void shouldHandleSchemaEvolutionWithTimestamps() {
      List<SinkRecord> records = new ArrayList<>();

      // First record with basic timestamp
      Map<String, Object> map1 = new HashMap<>();
      map1.put("id", 1);
      map1.put("created_at", "2025-09-25T18:05:12Z");
      records.add(new SinkRecord("test-topic", 0, null, null, null, map1, 0));

      // Second record adds a new timestamp field
      Map<String, Object> map2 = new HashMap<>();
      map2.put("id", 2);
      map2.put("created_at", "2025-09-25T19:05:12Z");
      map2.put("updated_at", "2025-09-25T20:05:12Z");
      records.add(new SinkRecord("test-topic", 0, null, null, null, map2, 1));

      try (VectorSchemaRoot result = converter.convertRecords(records)) {
        assertEquals(2, result.getRowCount());

        // Both timestamp fields should be in unified schema
        Field createdAtField = result.getSchema().findField("created_at");
        Field updatedAtField = result.getSchema().findField("updated_at");

        assertNotNull(createdAtField);
        assertNotNull(updatedAtField);
        assertInstanceOf(ArrowType.Timestamp.class, createdAtField.getType());
        assertInstanceOf(ArrowType.Timestamp.class, updatedAtField.getType());

        TimeStampMilliVector createdAtVector =
            (TimeStampMilliVector) result.getVector("created_at");
        TimeStampMilliVector updatedAtVector =
            (TimeStampMilliVector) result.getVector("updated_at");

        // First record should have created_at but null updated_at
        assertFalse(createdAtVector.isNull(0));
        assertTrue(updatedAtVector.isNull(0));

        // Second record should have both
        assertFalse(createdAtVector.isNull(1));
        assertFalse(updatedAtVector.isNull(1));
      }
    }
  }
}
