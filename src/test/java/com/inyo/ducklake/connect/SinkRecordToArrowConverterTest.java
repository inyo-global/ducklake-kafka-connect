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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SinkRecordToArrowConverterTest {

  // Constants to avoid magic numbers
  private static final int EXPECTED_ROW_COUNT = 1;
  private static final int EXPECTED_TAG_COUNT = 3;
  private static final int EXPECTED_MAP_ENTRIES = 2;
  private static final int TEST_ID_VALUE = 42;
  private static final int TEST_ZIP_CODE = 12345;
  private static final long TEST_LONG_ID = 7L;
  private static final int FIRST_ELEMENT_INDEX = 0;
  private static final int SECOND_ELEMENT_INDEX = 1;
  private static final int THIRD_ELEMENT_INDEX = 2;
  private static final int MAP_VALUE_A = 1;
  private static final int MAP_VALUE_B = 2;

  @Test
  @DisplayName("Converts primitive STRUCT fields")
  void testPrimitiveStruct() {
    // Given
    /*
     * Kafka Schema JSON representation:
     * {
     *   "type": "STRUCT",
     *   "fields": [
     *     {"name": "id", "type": "INT32", "optional": false},
     *     {"name": "name", "type": "STRING", "optional": false}
     *   ]
     * }
     */
    var kafkaSchema =
        SchemaBuilder.struct()
            .field("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
            .field("name", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
            .build();
    var struct = new Struct(kafkaSchema).put("id", TEST_ID_VALUE).put("name", "alice");

    // When
    SinkRecord rec = new SinkRecord("topic", 0, null, null, kafkaSchema, struct, 0);
    var converter = new SinkRecordToArrowConverter(new RootAllocator());
    var root = converter.convertRecords(List.of(rec));

    // Then
    /*
     * Expected Arrow Schema JSON representation:
     * {
     *   "fields": [
     *     {"name": "id", "type": {"name": "int", "bitWidth": 32, "isSigned": true}, "nullable": false},
     *     {"name": "name", "type": {"name": "utf8"}, "nullable": false}
     *   ]
     * }
     */
    assertEquals(EXPECTED_ROW_COUNT, root.getRowCount());
    IntVector idVec = (IntVector) root.getVector("id");
    VarCharVector nameVec = (VarCharVector) root.getVector("name");
    assertEquals(TEST_ID_VALUE, idVec.get(FIRST_ELEMENT_INDEX));
    assertEquals("alice", new String(nameVec.get(FIRST_ELEMENT_INDEX), StandardCharsets.UTF_8));
    root.close();
  }

  @Test
  @DisplayName("Converts ARRAY field")
  void testArrayField() {
    // Given
    /*
     * Kafka Schema JSON representation:
     * {
     *   "type": "STRUCT",
     *   "fields": [
     *     {
     *       "name": "tags",
     *       "type": "ARRAY",
     *       "valueType": "STRING",
     *       "optional": false
     *     }
     *   ]
     * }
     */
    var kafkaSchema =
        SchemaBuilder.struct()
            .field(
                "tags",
                SchemaBuilder.array(org.apache.kafka.connect.data.Schema.STRING_SCHEMA).build())
            .build();
    var struct = new Struct(kafkaSchema).put("tags", List.of("red", "green", "blue"));

    // When
    SinkRecord rec = new SinkRecord("topic", 0, null, null, kafkaSchema, struct, 0);
    var converter = new SinkRecordToArrowConverter(new RootAllocator());
    var root = converter.convertRecords(List.of(rec));

    // Then
    /*
     * Expected Arrow Schema JSON representation:
     * {
     *   "fields": [
     *     {
     *       "name": "tags",
     *       "type": {"name": "list"},
     *       "nullable": false,
     *       "children": [
     *         {"name": "element", "type": {"name": "utf8"}, "nullable": false}
     *       ]
     *     }
     *   ]
     * }
     */
    ListVector tags = (ListVector) root.getVector("tags");
    assertEquals(EXPECTED_ROW_COUNT, root.getRowCount());
    assertEquals(EXPECTED_TAG_COUNT, tags.getInnerValueCountAt(FIRST_ELEMENT_INDEX));
    VarCharVector data = (VarCharVector) tags.getDataVector();
    assertEquals("red", new String(data.get(FIRST_ELEMENT_INDEX), StandardCharsets.UTF_8));
    assertEquals("green", new String(data.get(SECOND_ELEMENT_INDEX), StandardCharsets.UTF_8));
    assertEquals("blue", new String(data.get(THIRD_ELEMENT_INDEX), StandardCharsets.UTF_8));
    root.close();
  }

  @Test
  @DisplayName("Converts MAP field")
  void testMapField() {
    // Given
    /*
     * Kafka Schema JSON representation:
     * {
     *   "type": "STRUCT",
     *   "fields": [
     *     {
     *       "name": "meta",
     *       "type": "MAP",
     *       "keyType": "STRING",
     *       "valueType": "INT32",
     *       "optional": false
     *     }
     *   ]
     * }
     */
    var kafkaSchema =
        SchemaBuilder.struct()
            .field(
                "meta",
                SchemaBuilder.map(
                        org.apache.kafka.connect.data.Schema.STRING_SCHEMA,
                        org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
                    .build())
            .build();
    Map<String, Integer> map = new LinkedHashMap<>();
    map.put("a", MAP_VALUE_A);
    map.put("b", MAP_VALUE_B);
    var struct = new Struct(kafkaSchema).put("meta", map);

    // When
    SinkRecord rec = new SinkRecord("topic", 0, null, null, kafkaSchema, struct, 0);
    var converter = new SinkRecordToArrowConverter(new RootAllocator());
    var root = converter.convertRecords(List.of(rec));

    // Then
    /*
     * Expected Arrow Schema JSON representation:
     * {
     *   "fields": [
     *     {
     *       "name": "meta",
     *       "type": {"name": "map", "keysSorted": false},
     *       "nullable": false,
     *       "children": [
     *         {
     *           "name": "entries",
     *           "type": {"name": "struct"},
     *           "nullable": false,
     *           "children": [
     *             {"name": "key", "type": {"name": "utf8"}, "nullable": false},
     *             {"name": "value", "type": {"name": "int", "bitWidth": 32, "isSigned": true}, "nullable": false}
     *           ]
     *         }
     *       ]
     *     }
     *   ]
     * }
     */
    MapVector mv = (MapVector) root.getVector("meta");
    assertEquals(EXPECTED_ROW_COUNT, root.getRowCount());
    StructVector entries = (StructVector) mv.getDataVector();
    assertEquals(EXPECTED_MAP_ENTRIES, entries.getValueCount());
    VarCharVector keyVec = (VarCharVector) entries.getChild("key");
    IntVector valVec = (IntVector) entries.getChild("value");
    assertEquals("a", new String(keyVec.get(FIRST_ELEMENT_INDEX), StandardCharsets.UTF_8));
    assertEquals(MAP_VALUE_A, valVec.get(FIRST_ELEMENT_INDEX));
    assertEquals("b", new String(keyVec.get(SECOND_ELEMENT_INDEX), StandardCharsets.UTF_8));
    assertEquals(MAP_VALUE_B, valVec.get(SECOND_ELEMENT_INDEX));
    root.close();
  }

  @Test
  @DisplayName("Converts nested STRUCT")
  void testNestedStruct() {
    // Given
    /*
     * Inner Address Schema JSON representation:
     * {
     *   "type": "STRUCT",
     *   "fields": [
     *     {"name": "street", "type": "STRING", "optional": false},
     *     {"name": "zip", "type": "INT32", "optional": false}
     *   ]
     * }
     */
    var inner =
        SchemaBuilder.struct()
            .field("street", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
            .field("zip", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
            .build();

    /*
     * Outer Schema JSON representation:
     * {
     *   "type": "STRUCT",
     *   "fields": [
     *     {"name": "id", "type": "INT64", "optional": false},
     *     {"name": "address", "type": "STRUCT", "fields": [...inner], "optional": false}
     *   ]
     * }
     */
    var outer =
        SchemaBuilder.struct()
            .field("id", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
            .field("address", inner)
            .build();

    var innerStruct = new Struct(inner).put("street", "Main").put("zip", TEST_ZIP_CODE);
    var outerStruct = new Struct(outer).put("id", TEST_LONG_ID).put("address", innerStruct);

    // When
    SinkRecord rec = new SinkRecord("topic", 0, null, null, outer, outerStruct, 0);
    var converter = new SinkRecordToArrowConverter(new RootAllocator());
    var root = converter.convertRecords(List.of(rec));

    // Then
    /*
     * Expected Arrow Schema JSON representation:
     * {
     *   "fields": [
     *     {"name": "id", "type": {"name": "int", "bitWidth": 64, "isSigned": true}, "nullable": false},
     *     {
     *       "name": "address",
     *       "type": {"name": "struct"},
     *       "nullable": false,
     *       "children": [
     *         {"name": "street", "type": {"name": "utf8"}, "nullable": false},
     *         {"name": "zip", "type": {"name": "int", "bitWidth": 32, "isSigned": true}, "nullable": false}
     *       ]
     *     }
     *   ]
     * }
     */
    BigIntVector idVec = (BigIntVector) root.getVector("id");
    StructVector addressVec = (StructVector) root.getVector("address");
    VarCharVector street = (VarCharVector) addressVec.getChild("street");
    IntVector zip = (IntVector) addressVec.getChild("zip");
    assertEquals(TEST_LONG_ID, idVec.get(FIRST_ELEMENT_INDEX));
    assertEquals("Main", new String(street.get(FIRST_ELEMENT_INDEX), StandardCharsets.UTF_8));
    assertEquals(TEST_ZIP_CODE, zip.get(FIRST_ELEMENT_INDEX));
    root.close();
  }

  @Test
  @DisplayName("Converts schemaless Map value by inferring schema")
  void testSchemalessMapValue() {
    // Given: a schemaless record where value is a Map
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("id", TEST_ID_VALUE);
    map.put("name", "dave");

    // When
    SinkRecord rec = new SinkRecord("topic", 0, null, null, null, map, 0);
    var converter = new SinkRecordToArrowConverter(new RootAllocator());
    var root = converter.convertRecords(List.of(rec));

    // Then
    assertEquals(EXPECTED_ROW_COUNT, root.getRowCount());
    IntVector idVec = (IntVector) root.getVector("id");
    VarCharVector nameVec = (VarCharVector) root.getVector("name");
    assertEquals(TEST_ID_VALUE, idVec.get(FIRST_ELEMENT_INDEX));
    assertEquals("dave", new String(nameVec.get(FIRST_ELEMENT_INDEX), StandardCharsets.UTF_8));
    root.close();
  }

  @Test
  @DisplayName("Converts schemaless JSON string value by parsing and inferring schema")
  void testSchemalessJsonStringValue() {
    // Given: a schemaless record where value is a JSON string
    String json = "{\"id\": 99, \"name\": \"eve\"}";

    // When
    SinkRecord rec = new SinkRecord("topic", 0, null, null, null, json, 0);
    var converter = new SinkRecordToArrowConverter(new RootAllocator());
    var root = converter.convertRecords(List.of(rec));

    // Then
    assertEquals(EXPECTED_ROW_COUNT, root.getRowCount());
    IntVector idVec = (IntVector) root.getVector("id");
    VarCharVector nameVec = (VarCharVector) root.getVector("name");
    assertEquals(99, idVec.get(FIRST_ELEMENT_INDEX));
    assertEquals("eve", new String(nameVec.get(FIRST_ELEMENT_INDEX), StandardCharsets.UTF_8));
    root.close();
  }

  @Test
  @DisplayName(
      "Handles large dataset that would cause IndexOutOfBoundsException without proper allocation")
  void testLargeDatasetBufferOverflow() {
    // Given: Many records with large string values that would exceed default buffer capacity
    var kafkaSchema =
        SchemaBuilder.struct()
            .field("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
            .field("large_text", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
            .build();

    // Create a large string that's approximately 1KB
    var largeString = "A".repeat(1024);

    // Create many records that would exceed the default 32KB buffer
    // With 1KB strings and 40 records, we'll exceed 32KB easily
    var records = new java.util.ArrayList<SinkRecord>();
    for (int i = 0; i < 40; i++) {
      var struct =
          new Struct(kafkaSchema)
              .put("id", i)
              .put("large_text", largeString + "_" + i); // Make each string unique
      records.add(new SinkRecord("topic", 0, null, null, kafkaSchema, struct, i));
    }

    // When: Converting these records should not throw IndexOutOfBoundsException
    var converter = new SinkRecordToArrowConverter(new RootAllocator());
    var root = converter.convertRecords(records);

    // Then: All records should be converted successfully
    assertEquals(40, root.getRowCount());

    // Verify first and last records
    IntVector idVec = (IntVector) root.getVector("id");
    VarCharVector textVec = (VarCharVector) root.getVector("large_text");

    assertEquals(0, idVec.get(0));
    assertEquals(39, idVec.get(39));

    var firstText = new String(textVec.get(0), StandardCharsets.UTF_8);
    var lastText = new String(textVec.get(39), StandardCharsets.UTF_8);

    assertEquals(largeString + "_0", firstText);
    assertEquals(largeString + "_39", lastText);

    root.close();
    converter.close();
  }

  @Test
  @DisplayName("Enhanced error messaging with actual sample values for struct vs string conflict")
  void testEnhancedErrorMessagingStructVsString() {
    // Given: Two records with conflicting types for the same field
    var structSchema =
        SchemaBuilder.struct()
            .field(
                "address",
                SchemaBuilder.struct()
                    .field("street", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                    .field("city", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                    .build())
            .build();

    var stringSchema =
        SchemaBuilder.struct()
            .field("address", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
            .build();

    var addressStruct =
        new Struct(structSchema.field("address").schema())
            .put("street", "123 Main St")
            .put("city", "New York");

    var record1 = new Struct(structSchema).put("address", addressStruct);
    var record2 = new Struct(stringSchema).put("address", "456 Oak Avenue");

    var sinkRecord1 = new SinkRecord("topic", 0, null, null, structSchema, record1, 0);
    var sinkRecord2 = new SinkRecord("topic", 0, null, null, stringSchema, record2, 1);

    // When & Then: Should throw exception with actual sample values
    var converter = new SinkRecordToArrowConverter(new RootAllocator());
    var exception =
        assertThrows(
            RuntimeException.class,
            () -> converter.convertRecords(Arrays.asList(sinkRecord1, sinkRecord2)));

    // Verify the enhanced error message contains actual sample values
    var message = exception.getCause().getMessage();
    assertTrue(message.contains("Sample values:"));
    assertTrue(message.contains("123 Main St") || message.contains("street"));
    assertTrue(message.contains("456 Oak Avenue"));
    assertTrue(message.contains("address"));

    converter.close();
  }

  @Test
  @DisplayName("Enhanced error messaging with actual sample values for integer vs string conflict")
  void testEnhancedErrorMessagingIntVsString() {
    // Given: Two records with conflicting types for the same field
    var intSchema =
        SchemaBuilder.struct()
            .field("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
            .build();

    var stringSchema =
        SchemaBuilder.struct()
            .field("id", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
            .build();

    var record1 = new Struct(intSchema).put("id", 42);
    var record2 = new Struct(stringSchema).put("id", "string-id-123");

    var sinkRecord1 = new SinkRecord("topic", 0, null, null, intSchema, record1, 0);
    var sinkRecord2 = new SinkRecord("topic", 0, null, null, stringSchema, record2, 1);

    // When & Then: Should throw exception with actual sample values
    var converter = new SinkRecordToArrowConverter(new RootAllocator());
    var exception =
        assertThrows(
            RuntimeException.class,
            () -> converter.convertRecords(Arrays.asList(sinkRecord1, sinkRecord2)));

    // Verify the enhanced error message contains actual sample values
    var message = exception.getCause().getMessage();
    assertTrue(message.contains("Sample values:"));
    assertTrue(message.contains("42"));
    assertTrue(message.contains("string-id-123"));
    assertTrue(message.contains("id"));

    converter.close();
  }

  @Test
  @DisplayName("Enhanced error messaging with boolean vs float conflict")
  void testEnhancedErrorMessagingBoolVsFloat() {
    // Given: Records with boolean vs string conflict (truly incompatible types)
    var boolSchema =
        SchemaBuilder.struct()
            .field("flag", org.apache.kafka.connect.data.Schema.BOOLEAN_SCHEMA)
            .build();

    var stringSchema =
        SchemaBuilder.struct()
            .field("flag", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
            .build();

    var record1 = new Struct(boolSchema).put("flag", true);
    var record2 = new Struct(stringSchema).put("flag", "not-a-boolean");

    var sinkRecord1 = new SinkRecord("topic", 0, null, null, boolSchema, record1, 0);
    var sinkRecord2 = new SinkRecord("topic", 0, null, null, stringSchema, record2, 1);

    // When & Then: Should throw exception with detailed type information
    var converter = new SinkRecordToArrowConverter(new RootAllocator());
    var exception =
        assertThrows(
            RuntimeException.class,
            () -> converter.convertRecords(Arrays.asList(sinkRecord1, sinkRecord2)));

    // Verify the enhanced error message contains both sample values and type descriptions
    var message = exception.getCause().getMessage();
    assertTrue(message.contains("Sample values:"));
    assertTrue(message.contains("true") || message.contains("not-a-boolean"));
    assertTrue(message.contains("flag"));

    converter.close();
  }

  @Test
  @DisplayName("Enhanced error messaging with nested struct sample values")
  void testEnhancedErrorMessagingNestedStruct() {
    // Given: Records with conflicting nested field types
    var nestedStructSchema =
        SchemaBuilder.struct()
            .field(
                "user",
                SchemaBuilder.struct()
                    .field(
                        "profile",
                        SchemaBuilder.struct()
                            .field("age", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
                            .build())
                    .build())
            .build();

    var simpleSchema =
        SchemaBuilder.struct()
            .field(
                "user",
                SchemaBuilder.struct()
                    .field("profile", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                    .build())
            .build();

    var profileStruct =
        new Struct(nestedStructSchema.field("user").schema().field("profile").schema())
            .put("age", 25);
    var userStruct =
        new Struct(nestedStructSchema.field("user").schema()).put("profile", profileStruct);
    var record1 = new Struct(nestedStructSchema).put("user", userStruct);

    var simpleUserStruct =
        new Struct(simpleSchema.field("user").schema()).put("profile", "Simple profile string");
    var record2 = new Struct(simpleSchema).put("user", simpleUserStruct);

    var sinkRecord1 = new SinkRecord("topic", 0, null, null, nestedStructSchema, record1, 0);
    var sinkRecord2 = new SinkRecord("topic", 0, null, null, simpleSchema, record2, 1);

    // When & Then: Should throw exception with sample values for nested fields
    var converter = new SinkRecordToArrowConverter(new RootAllocator());
    var exception =
        assertThrows(
            RuntimeException.class,
            () -> converter.convertRecords(Arrays.asList(sinkRecord1, sinkRecord2)));

    // Verify the enhanced error message contains sample values
    var message = exception.getCause().getMessage();
    assertTrue(message.contains("profile")); // Field name should be present
    assertTrue(
        message.contains("Struct")
            || message.contains("Utf8")); // Type information should be present

    converter.close();
  }

  @Test
  @DisplayName("Sample value collection limits values to prevent memory issues")
  void testSampleValueCollectionLimiting() {
    // Given: Many records with the same conflicting field to test limiting
    var intSchema =
        SchemaBuilder.struct()
            .field("value", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
            .build();

    var stringSchema =
        SchemaBuilder.struct()
            .field("value", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
            .build();

    var records = new ArrayList<SinkRecord>();

    // Add 10 integer records
    for (int i = 0; i < 10; i++) {
      var record = new Struct(intSchema).put("value", i);
      records.add(new SinkRecord("topic", 0, null, null, intSchema, record, i));
    }

    // Add 1 string record to cause conflict
    var stringRecord = new Struct(stringSchema).put("value", "conflict-string");
    records.add(new SinkRecord("topic", 0, null, null, stringSchema, stringRecord, 10));

    // When & Then: Should throw exception but limit sample values
    var converter = new SinkRecordToArrowConverter(new RootAllocator());
    var exception = assertThrows(RuntimeException.class, () -> converter.convertRecords(records));

    // Verify error message contains sample values but not all 10 integers
    var message = exception.getCause().getMessage();
    assertTrue(message.contains("Sample values:"));
    assertFalse(message.contains("6"));
    // Should contain some but not all integer values due to limiting

    converter.close();
  }

  @Test
  @DisplayName("Enhanced error messaging works with schemaless records")
  void testEnhancedErrorMessagingSchemaless() {
    // Given: Schemaless records with conflicting types
    Map<String, Object> record1 = new LinkedHashMap<>();
    record1.put("data", Map.of("key", "value")); // Complex object

    Map<String, Object> record2 = new LinkedHashMap<>();
    record2.put("data", "simple string"); // Simple string

    var sinkRecord1 = new SinkRecord("topic", 0, null, null, null, record1, 0);
    var sinkRecord2 = new SinkRecord("topic", 0, null, null, null, record2, 1);

    // When & Then: Should throw exception with sample values from schemaless data
    var converter = new SinkRecordToArrowConverter(new RootAllocator());
    var exception =
        assertThrows(
            RuntimeException.class,
            () -> converter.convertRecords(Arrays.asList(sinkRecord1, sinkRecord2)));

    // Verify the enhanced error message contains actual sample values
    var message = exception.getCause().getMessage();
    assertTrue(message.contains("Sample values:"));
    assertTrue(message.contains("simple string"));
    assertTrue(message.contains("data"));

    converter.close();
  }

  @Test
  @DisplayName("Handles null values in schema inference without type conflicts")
  void testNullValueHandlingInSchemaInference() {
    // Given: Records with mixed null and non-null complex fields using same schema structure
    var addressSchema =
        SchemaBuilder.struct()
            .field("id", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
            .field("street", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
            .field("city", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
            .field("zipcode", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
            .optional()
            .build();

    var personSchema =
        SchemaBuilder.struct()
            .field("id", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
            .field("firstName", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
            .field("lastName", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
            .optional()
            .build();

    var recordSchema =
        SchemaBuilder.struct()
            .field("recordId", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
            .field("person", personSchema)
            .field("address", addressSchema) // Both records use same structured schema
            .optional()
            .build();

    // Record 1: With populated address
    var addressStruct =
        new Struct(addressSchema)
            .put("id", "addr-123")
            .put("street", "123 Main St")
            .put("city", "Springfield")
            .put("zipcode", "12345");

    var personStruct =
        new Struct(personSchema)
            .put("id", "person-123")
            .put("firstName", "John")
            .put("lastName", "Doe");

    var record1Value =
        new Struct(recordSchema)
            .put("recordId", "record-1")
            .put("person", personStruct)
            .put("address", addressStruct);

    // Record 2: With null address (same schema structure)
    var personStruct2 =
        new Struct(personSchema)
            .put("id", "person-456")
            .put("firstName", "Jane")
            .put("lastName", "Smith");

    var record2Value =
        new Struct(recordSchema)
            .put("recordId", "record-2")
            .put("person", personStruct2)
            .put("address", null); // NULL address but same schema

    // When
    var record1 = new SinkRecord("test-topic", 0, null, "key1", recordSchema, record1Value, 1);
    var record2 = new SinkRecord("test-topic", 0, null, "key2", recordSchema, record2Value, 2);
    var records = Arrays.asList(record1, record2);

    var converter = new SinkRecordToArrowConverter(new RootAllocator());
    var root = converter.convertRecords(records);

    // Then
    assertEquals(2, root.getRowCount());

    // Verify all expected fields are present
    var idVec = (VarCharVector) root.getVector("recordId");
    var personVec = (StructVector) root.getVector("person");
    var addressVec = (StructVector) root.getVector("address");

    // Check first record
    assertEquals("record-1", new String(idVec.get(0), StandardCharsets.UTF_8));
    assertFalse(personVec.isNull(0));
    assertFalse(addressVec.isNull(0));

    // Check second record (with null address)
    assertEquals("record-2", new String(idVec.get(1), StandardCharsets.UTF_8));
    assertFalse(personVec.isNull(1));
    assertTrue(addressVec.isNull(1)); // Address should be null

    root.close();
    converter.close();
  }

  @Test
  @DisplayName("Handles schema inference from maps with null values correctly")
  void testSchemaInferenceFromMapsWithNullValues() {
    // Given: Schemaless records (maps) with null values that should not be inferred as strings
    var record1Map = new LinkedHashMap<String, Object>();
    record1Map.put("id", "record-123");
    record1Map.put("name", "Test Record");
    record1Map.put("count", 42);
    record1Map.put("active", true);
    record1Map.put("metadata", null); // This should not cause type conflicts

    var record2Map = new LinkedHashMap<String, Object>();
    record2Map.put("id", "record-456");
    record2Map.put("name", null); // Different field is null
    record2Map.put("count", 84);
    record2Map.put("active", false);
    record2Map.put("metadata", "actual metadata");

    // When
    var record1 = new SinkRecord("test-topic", 0, null, "key1", null, record1Map, 1);
    var record2 = new SinkRecord("test-topic", 0, null, "key2", null, record2Map, 2);
    var records = Arrays.asList(record1, record2);

    var converter = new SinkRecordToArrowConverter(new RootAllocator());
    var root = converter.convertRecords(records);

    // Then: Should successfully convert without type conflicts
    assertEquals(2, root.getRowCount());

    // Verify inferred schema includes fields with actual types
    var idVec = (VarCharVector) root.getVector("id");
    var countVec = (IntVector) root.getVector("count");
    var metadataVec = (VarCharVector) root.getVector("metadata");

    assertEquals("record-123", new String(idVec.get(0), StandardCharsets.UTF_8));
    assertEquals(42, countVec.get(0));
    assertEquals("record-456", new String(idVec.get(1), StandardCharsets.UTF_8));
    assertEquals(84, countVec.get(1));
    assertEquals("actual metadata", new String(metadataVec.get(1), StandardCharsets.UTF_8));

    root.close();
    converter.close();
  }

  @Test
  @DisplayName("Handles nested structures with mixed null values")
  void testNestedStructuresWithMixedNullValues() {
    // Given: Nested schema with mixed null and non-null values
    var contactSchema =
        SchemaBuilder.struct()
            .field("email", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
            .field("phone", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
            .optional()
            .build();

    var companySchema =
        SchemaBuilder.struct()
            .field("name", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
            .field("contact", contactSchema)
            .optional()
            .build();

    var employeeSchema =
        SchemaBuilder.struct()
            .field("id", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
            .field("name", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
            .field("company", companySchema)
            .optional()
            .build();

    // Record 1: Full structure
    var contactStruct1 =
        new Struct(contactSchema).put("email", "contact@acme.com").put("phone", "555-0123");
    var companyStruct1 =
        new Struct(companySchema).put("name", "ACME Corp").put("contact", contactStruct1);
    var employee1 =
        new Struct(employeeSchema)
            .put("id", "emp-001")
            .put("name", "Alice Johnson")
            .put("company", companyStruct1);

    // Record 2: Null company
    var employee2 =
        new Struct(employeeSchema)
            .put("id", "emp-002")
            .put("name", "Bob Smith")
            .put("company", null);

    // Record 3: Company with null contact
    var companyStruct3 = new Struct(companySchema).put("name", "XYZ Ltd").put("contact", null);
    var employee3 =
        new Struct(employeeSchema)
            .put("id", "emp-003")
            .put("name", "Charlie Brown")
            .put("company", companyStruct3);

    // When
    var record1 = new SinkRecord("employees", 0, null, "key1", employeeSchema, employee1, 1);
    var record2 = new SinkRecord("employees", 0, null, "key2", employeeSchema, employee2, 2);
    var record3 = new SinkRecord("employees", 0, null, "key3", employeeSchema, employee3, 3);
    var records = Arrays.asList(record1, record2, record3);

    var converter = new SinkRecordToArrowConverter(new RootAllocator());
    var root = converter.convertRecords(records);

    // Then: Should handle mixed null/non-null nested structures
    assertEquals(3, root.getRowCount());

    var idVec = (VarCharVector) root.getVector("id");
    var nameVec = (VarCharVector) root.getVector("name");
    var companyVec = (StructVector) root.getVector("company");

    // Verify first record (full structure)
    assertEquals("emp-001", new String(idVec.get(0), StandardCharsets.UTF_8));
    assertEquals("Alice Johnson", new String(nameVec.get(0), StandardCharsets.UTF_8));
    assertFalse(companyVec.isNull(0));

    // Verify second record (null company)
    assertEquals("emp-002", new String(idVec.get(1), StandardCharsets.UTF_8));
    assertEquals("Bob Smith", new String(nameVec.get(1), StandardCharsets.UTF_8));
    assertTrue(companyVec.isNull(1));

    // Verify third record (company with null contact)
    assertEquals("emp-003", new String(idVec.get(2), StandardCharsets.UTF_8));
    assertEquals("Charlie Brown", new String(nameVec.get(2), StandardCharsets.UTF_8));
    assertFalse(companyVec.isNull(2));

    root.close();
    converter.close();
  }

  @Test
  @DisplayName(
      "Converts arrays containing Maps to arrays of Structs when schema expects Struct elements")
  void testArrayWithMapElementsConvertedToStructs() {
    // Given: JSON data with an array containing Map objects (typical from JSON parsing)
    var jsonPayload =
        """
        {
          "id": "31c68ce2-eb6b-440c-adca-73979bf2d65f",
          "amount": "50.38",
          "product": "BILLPAY",
          "validationErrors": [
            {
              "field": "amount",
              "message": "Amount must be positive",
              "code": "INVALID_AMOUNT"
            },
            {
              "field": "client.person.firstName",
              "message": "First name is required",
              "code": "REQUIRED_FIELD"
            }
          ]
        }
        """;

    var objectMapper = new com.fasterxml.jackson.databind.ObjectMapper();
    Map<String, Object> valueMap;
    try {
      valueMap =
          objectMapper.readValue(
              jsonPayload, new com.fasterxml.jackson.core.type.TypeReference<>() {});
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    // When: Converting schemaless JSON data (simulates real-world Kafka Connect scenario)
    var sinkRecord = new SinkRecord("test-topic", 0, null, "test-key", null, valueMap, 1L);
    var records = List.of(sinkRecord);

    try (var allocator = new RootAllocator();
        var converter = new SinkRecordToArrowConverter(allocator)) {

      // Then: Should successfully convert without throwing DataException
      var result = converter.convertRecords(records);

      // Verify the conversion completed successfully
      assertEquals(EXPECTED_ROW_COUNT, result.getRowCount());

      // Verify the validationErrors field was converted properly
      ListVector validationErrorsVector = (ListVector) result.getVector("validationErrors");
      assertEquals(2, validationErrorsVector.getInnerValueCountAt(FIRST_ELEMENT_INDEX));

      // Verify the array elements are now Structs with proper fields
      StructVector dataVector = (StructVector) validationErrorsVector.getDataVector();
      VarCharVector fieldVector = (VarCharVector) dataVector.getChild("field");
      VarCharVector messageVector = (VarCharVector) dataVector.getChild("message");
      VarCharVector codeVector = (VarCharVector) dataVector.getChild("code");

      // Check first validation error
      assertEquals(
          "amount", new String(fieldVector.get(FIRST_ELEMENT_INDEX), StandardCharsets.UTF_8));
      assertEquals(
          "Amount must be positive",
          new String(messageVector.get(FIRST_ELEMENT_INDEX), StandardCharsets.UTF_8));
      assertEquals(
          "INVALID_AMOUNT",
          new String(codeVector.get(FIRST_ELEMENT_INDEX), StandardCharsets.UTF_8));

      // Check second validation error
      assertEquals(
          "client.person.firstName",
          new String(fieldVector.get(SECOND_ELEMENT_INDEX), StandardCharsets.UTF_8));
      assertEquals(
          "First name is required",
          new String(messageVector.get(SECOND_ELEMENT_INDEX), StandardCharsets.UTF_8));
      assertEquals(
          "REQUIRED_FIELD",
          new String(codeVector.get(SECOND_ELEMENT_INDEX), StandardCharsets.UTF_8));

      result.close();
    }
  }

  @Test
  @DisplayName("Handles empty arrays with Struct element schema")
  void testEmptyArrayWithStructElementSchema() {
    // Given: JSON data with an empty validationErrors array
    var jsonPayload =
        """
        {
          "id": "test-id",
          "amount": "25.00",
          "validationErrors": []
        }
        """;

    var objectMapper = new com.fasterxml.jackson.databind.ObjectMapper();
    Map<String, Object> valueMap;
    try {
      valueMap =
          objectMapper.readValue(
              jsonPayload, new com.fasterxml.jackson.core.type.TypeReference<>() {});
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    // When: Converting schemaless JSON data with empty array
    var sinkRecord = new SinkRecord("test-topic", 0, null, "test-key", null, valueMap, 1L);
    var records = List.of(sinkRecord);

    try (var allocator = new RootAllocator();
        var converter = new SinkRecordToArrowConverter(allocator)) {

      // Then: Should successfully convert empty array
      var result = converter.convertRecords(records);

      assertEquals(EXPECTED_ROW_COUNT, result.getRowCount());

      // Verify the conversion completed successfully - for empty arrays, the field might not be
      // created
      // or might be inferred as a different type since there are no elements to determine the
      // structure
      var schema = result.getSchema();
      assertNotNull(schema);

      // The key point is that conversion succeeds without throwing an exception
      result.close();
    }
  }

  @Test
  @DisplayName("Converts mixed records with different array content")
  void testMixedRecordsWithDifferentArrayContent() {
    // Given: Multiple records with different validationErrors content
    var jsonPayload1 =
        """
        {
          "id": "record1",
          "validationErrors": [
            {"field": "amount", "message": "Amount error", "code": "ERROR_1"}
          ]
        }
        """;

    var jsonPayload2 =
        """
        {
          "id": "record2",
          "validationErrors": []
        }
        """;

    var objectMapper = new com.fasterxml.jackson.databind.ObjectMapper();
    Map<String, Object> valueMap1;
    Map<String, Object> valueMap2;
    try {
      valueMap1 =
          objectMapper.readValue(
              jsonPayload1, new com.fasterxml.jackson.core.type.TypeReference<>() {});
      valueMap2 =
          objectMapper.readValue(
              jsonPayload2, new com.fasterxml.jackson.core.type.TypeReference<>() {});
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    // When: Converting multiple records with different array contents
    var records =
        List.of(
            new SinkRecord("test-topic", 0, null, "key1", null, valueMap1, 1L),
            new SinkRecord("test-topic", 0, null, "key2", null, valueMap2, 2L));

    try (var allocator = new RootAllocator();
        var converter = new SinkRecordToArrowConverter(allocator)) {

      // Then: Should successfully convert all records with unified schema
      var result = converter.convertRecords(records);

      assertEquals(2, result.getRowCount());

      // Verify unified schema handles both populated and empty arrays
      ListVector validationErrorsVector = (ListVector) result.getVector("validationErrors");
      assertEquals(
          1,
          validationErrorsVector.getInnerValueCountAt(
              FIRST_ELEMENT_INDEX)); // First record has 1 error
      assertEquals(
          0,
          validationErrorsVector.getInnerValueCountAt(
              SECOND_ELEMENT_INDEX)); // Second record has 0 errors

      result.close();
    }
  }

  @Test
  @DisplayName("Handles empty nested STRUCT (no fields)")
  void testEmptyNestedStruct() {
    // Given: outer struct has id and address (empty struct)
    var emptyAddress = SchemaBuilder.struct().build();
    var outer =
        SchemaBuilder.struct()
            .field("id", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
            .field("address", emptyAddress)
            .build();

    var addressStruct = new Struct(emptyAddress); // no fields set
    var outerStruct = new Struct(outer).put("id", 123L).put("address", addressStruct);

    // When
    var rec = new SinkRecord("topic", 0, null, null, outer, outerStruct, 0);
    var converter = new SinkRecordToArrowConverter(new org.apache.arrow.memory.RootAllocator());
    var root = converter.convertRecords(List.of(rec));

    // Then: empty struct field should be pruned from unified schema
    assertEquals(1, root.getRowCount());
    var idVec = (org.apache.arrow.vector.BigIntVector) root.getVector("id");
    assertEquals(123L, idVec.get(0));

    var addressVec = root.getVector("address");
    assertTrue(addressVec == null, "address field should be removed from schema");

    root.close();
    converter.close();
  }
}
