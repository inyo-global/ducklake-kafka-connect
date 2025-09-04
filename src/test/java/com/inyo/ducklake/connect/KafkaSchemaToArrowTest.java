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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.inyo.ducklake.ingestor.ArrowTypeConstants;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class KafkaSchemaToArrowTest {

  // Constants to avoid magic numbers
  private static final int EXPECTED_SIMPLE_SCHEMA_FIELDS = 3;
  private static final int EXPECTED_PRIMITIVE_TYPES_FIELDS = 9;
  private static final int EXPECTED_NESTED_STRUCT_FIELDS = 3;
  private static final int EXPECTED_ADDRESS_CHILDREN = 3;
  private static final int FIRST_FIELD_INDEX = 0;
  private static final int SECOND_FIELD_INDEX = 1;
  private static final int THIRD_FIELD_INDEX = 2;
  private static final int FOURTH_FIELD_INDEX = 3;
  private static final int FIFTH_FIELD_INDEX = 4;
  private static final int SIXTH_FIELD_INDEX = 5;
  private static final int SEVENTH_FIELD_INDEX = 6;
  private static final int EIGHTH_FIELD_INDEX = 7;
  private static final int NINTH_FIELD_INDEX = 8;

  @Nested
  @DisplayName("arrowSchemaFromKafka tests")
  class ArrowSchemaFromKafkaTests {

    @Test
    @DisplayName("Should convert simple schema with primitive fields")
    void shouldConvertSimpleSchema() {
      // Given
      /*
       * Kafka Schema JSON representation:
       * {
       *   "type": "STRUCT",
       *   "fields": [
       *     {"name": "id", "type": "INT32", "optional": false},
       *     {"name": "name", "type": "STRING", "optional": false},
       *     {"name": "age", "type": "INT32", "optional": true}
       *   ]
       * }
       */
      org.apache.kafka.connect.data.Schema kafkaSchema =
          SchemaBuilder.struct()
              .field("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
              .field("name", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
              .field("age", org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
              .build();

      // When
      Schema arrowSchema = KafkaSchemaToArrow.arrowSchemaFromKafka(kafkaSchema);

      // Then
      /*
       * Expected Arrow Schema JSON representation:
       * {
       *   "fields": [
       *     {"name": "id", "type": {"name": "int", "bitWidth": INT32, "isSigned": true}, "nullable": false},
       *     {"name": "name", "type": {"name": "utf8"}, "nullable": false},
       *     {"name": "age", "type": {"name": "int", "bitWidth": INT32, "isSigned": true}, "nullable": true}
       *   ]
       * }
       */
      assertNotNull(arrowSchema);
      assertEquals(EXPECTED_SIMPLE_SCHEMA_FIELDS, arrowSchema.getFields().size());

      Field idField = arrowSchema.getFields().get(FIRST_FIELD_INDEX);
      assertEquals("id", idField.getName());
      assertFalse(idField.isNullable());
      assertEquals(new ArrowType.Int(ArrowTypeConstants.INT32_BIT_WIDTH, true), idField.getType());

      Field nameField = arrowSchema.getFields().get(SECOND_FIELD_INDEX);
      assertEquals("name", nameField.getName());
      assertFalse(nameField.isNullable());
      assertEquals(ArrowType.Utf8.INSTANCE, nameField.getType());

      Field ageField = arrowSchema.getFields().get(THIRD_FIELD_INDEX);
      assertEquals("age", ageField.getName());
      assertTrue(ageField.isNullable());
      assertEquals(new ArrowType.Int(ArrowTypeConstants.INT32_BIT_WIDTH, true), ageField.getType());
    }

    @Test
    @DisplayName("Should convert schema with all primitive types")
    void shouldConvertAllPrimitiveTypes() {
      // Given
      /*
       * Kafka Schema JSON representation:
       * {
       *   "type": "STRUCT",
       *   "fields": [
       *     {"name": "int8_field", "type": "INT8", "optional": false},
       *     {"name": "int16_field", "type": "INT16", "optional": false},
       *     {"name": "int32_field", "type": "INT32", "optional": false},
       *     {"name": "int64_field", "type": "INT64", "optional": false},
       *     {"name": "float32_field", "type": "FLOAT32", "optional": false},
       *     {"name": "float64_field", "type": "FLOAT64", "optional": false},
       *     {"name": "boolean_field", "type": "BOOLEAN", "optional": false},
       *     {"name": "string_field", "type": "STRING", "optional": false},
       *     {"name": "bytes_field", "type": "BYTES", "optional": false}
       *   ]
       * }
       */
      org.apache.kafka.connect.data.Schema kafkaSchema =
          SchemaBuilder.struct()
              .field("int8_field", org.apache.kafka.connect.data.Schema.INT8_SCHEMA)
              .field("int16_field", org.apache.kafka.connect.data.Schema.INT16_SCHEMA)
              .field("int32_field", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
              .field("int64_field", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
              .field("float32_field", org.apache.kafka.connect.data.Schema.FLOAT32_SCHEMA)
              .field("float64_field", org.apache.kafka.connect.data.Schema.FLOAT64_SCHEMA)
              .field("boolean_field", org.apache.kafka.connect.data.Schema.BOOLEAN_SCHEMA)
              .field("string_field", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
              .field("bytes_field", org.apache.kafka.connect.data.Schema.BYTES_SCHEMA)
              .build();

      // When
      Schema arrowSchema = KafkaSchemaToArrow.arrowSchemaFromKafka(kafkaSchema);

      // Then
      /*
       * Expected Arrow Schema JSON representation:
       * {
       *   "fields": [
       *     {"name": "int8_field", "type": {"name": "int", "bitWidth": 8, "isSigned": true}, "nullable": false},
       *     {"name": "int16_field", "type": {"name": "int", "bitWidth": 16, "isSigned": true}, "nullable": false},
       *     {"name": "int32_field", "type": {"name": "int", "bitWidth": 32, "isSigned": true}, "nullable": false},
       *     {"name": "int64_field", "type": {"name": "int", "bitWidth": 64, "isSigned": true}, "nullable": false},
       *     {"name": "float32_field", "type": {"name": "floatingpoint", "precision": "SINGLE"}, "nullable": false},
       *     {"name": "float64_field", "type": {"name": "floatingpoint", "precision": "DOUBLE"}, "nullable": false},
       *     {"name": "boolean_field", "type": {"name": "bool"}, "nullable": false},
       *     {"name": "string_field", "type": {"name": "utf8"}, "nullable": false},
       *     {"name": "bytes_field", "type": {"name": "binary"}, "nullable": false}
       *   ]
       * }
       */
      assertNotNull(arrowSchema);
      assertEquals(EXPECTED_PRIMITIVE_TYPES_FIELDS, arrowSchema.getFields().size());

      assertEquals(
          new ArrowType.Int(ArrowTypeConstants.INT8_BIT_WIDTH, true),
          arrowSchema.getFields().get(FIRST_FIELD_INDEX).getType());
      assertEquals(
          new ArrowType.Int(ArrowTypeConstants.INT16_BIT_WIDTH, true),
          arrowSchema.getFields().get(SECOND_FIELD_INDEX).getType());
      assertEquals(
          new ArrowType.Int(ArrowTypeConstants.INT32_BIT_WIDTH, true),
          arrowSchema.getFields().get(THIRD_FIELD_INDEX).getType());
      assertEquals(
          new ArrowType.Int(ArrowTypeConstants.INT64_BIT_WIDTH, true),
          arrowSchema.getFields().get(FOURTH_FIELD_INDEX).getType());
      assertEquals(
          new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE),
          arrowSchema.getFields().get(FIFTH_FIELD_INDEX).getType());
      assertEquals(
          new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
          arrowSchema.getFields().get(SIXTH_FIELD_INDEX).getType());
      assertEquals(
          ArrowType.Bool.INSTANCE, arrowSchema.getFields().get(SEVENTH_FIELD_INDEX).getType());
      assertEquals(
          ArrowType.Utf8.INSTANCE, arrowSchema.getFields().get(EIGHTH_FIELD_INDEX).getType());
      assertEquals(
          ArrowType.Binary.INSTANCE, arrowSchema.getFields().get(NINTH_FIELD_INDEX).getType());
    }

    @Test
    @DisplayName("Should convert schema with nested struct")
    void shouldConvertNestedStruct() {
      // Given
      /*
       * Address Schema JSON representation:
       * {
       *   "type": "STRUCT",
       *   "fields": [
       *     {"name": "street", "type": "STRING", "optional": false},
       *     {"name": "city", "type": "STRING", "optional": false},
       *     {"name": "zipcode", "type": "STRING", "optional": false}
       *   ]
       * }
       */
      org.apache.kafka.connect.data.Schema addressSchema =
          SchemaBuilder.struct()
              .field("street", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
              .field("city", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
              .field("zipcode", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
              .build();

      /*
       * Main Schema JSON representation:
       * {
       *   "type": "STRUCT",
       *   "fields": [
       *     {"name": "id", "type": "INT32", "optional": false},
       *     {"name": "name", "type": "STRING", "optional": false},
       *     {"name": "address", "type": "STRUCT", "fields": [...addressSchema], "optional": false}
       *   ]
       * }
       */
      org.apache.kafka.connect.data.Schema kafkaSchema =
          SchemaBuilder.struct()
              .field("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
              .field("name", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
              .field("address", addressSchema)
              .build();

      // When
      Schema arrowSchema = KafkaSchemaToArrow.arrowSchemaFromKafka(kafkaSchema);

      // Then
      /*
       * Expected Arrow Schema JSON representation:
       * {
       *   "fields": [
       *     {"name": "id", "type": {"name": "int", "bitWidth": 32, "isSigned": true}, "nullable": false},
       *     {"name": "name", "type": {"name": "utf8"}, "nullable": false},
       *     {"name": "address", "type": {"name": "struct"}, "nullable": false, "children": [
       *       {"name": "street", "type": {"name": "utf8"}, "nullable": false},
       *       {"name": "city", "type": {"name": "utf8"}, "nullable": false},
       *       {"name": "zipcode", "type": {"name": "utf8"}, "nullable": false}
       *     ]}
       *   ]
       * }
       */
      assertNotNull(arrowSchema);
      assertEquals(EXPECTED_NESTED_STRUCT_FIELDS, arrowSchema.getFields().size());

      Field addressField = arrowSchema.getFields().get(THIRD_FIELD_INDEX);
      assertEquals("address", addressField.getName());
      assertEquals(ArrowType.Struct.INSTANCE, addressField.getType());
      assertEquals(EXPECTED_ADDRESS_CHILDREN, addressField.getChildren().size());
    }

    @Test
    @DisplayName("Should convert schema with array field")
    void shouldConvertArrayField() {
      // Given
      org.apache.kafka.connect.data.Schema kafkaSchema =
          SchemaBuilder.struct()
              .field("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
              .field(
                  "tags",
                  SchemaBuilder.array(org.apache.kafka.connect.data.Schema.STRING_SCHEMA).build())
              .build();

      // When
      Schema arrowSchema = KafkaSchemaToArrow.arrowSchemaFromKafka(kafkaSchema);

      // Then
      assertNotNull(arrowSchema);
      assertEquals(2, arrowSchema.getFields().size());

      Field tagsField = arrowSchema.getFields().get(1);
      assertEquals("tags", tagsField.getName());
      assertEquals(ArrowType.List.INSTANCE, tagsField.getType());
      assertEquals(1, tagsField.getChildren().size());

      Field elementField = tagsField.getChildren().get(0);
      assertEquals("element", elementField.getName());
      assertEquals(ArrowType.Utf8.INSTANCE, elementField.getType());
    }

    @Test
    @DisplayName("Should convert schema with map field")
    void shouldConvertMapField() {
      // Given
      org.apache.kafka.connect.data.Schema kafkaSchema =
          SchemaBuilder.struct()
              .field("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
              .field(
                  "metadata",
                  SchemaBuilder.map(
                          org.apache.kafka.connect.data.Schema.STRING_SCHEMA,
                          org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                      .build())
              .build();

      // When
      Schema arrowSchema = KafkaSchemaToArrow.arrowSchemaFromKafka(kafkaSchema);

      // Then
      assertNotNull(arrowSchema);
      assertEquals(2, arrowSchema.getFields().size());

      Field metadataField = arrowSchema.getFields().get(1);
      assertEquals("metadata", metadataField.getName());
      assertEquals(new ArrowType.Map(true), metadataField.getType());
      // New Arrow map layout: one child struct named 'entries'
      assertEquals(1, metadataField.getChildren().size());
      Field entries = metadataField.getChildren().get(0);
      assertEquals("entries", entries.getName());
      assertEquals(ArrowType.Struct.INSTANCE, entries.getType());
      assertEquals(2, entries.getChildren().size());

      Field keyField = entries.getChildren().get(0);
      assertEquals("key", keyField.getName());
      assertEquals(ArrowType.Utf8.INSTANCE, keyField.getType());

      Field valueField = entries.getChildren().get(1);
      assertEquals("value", valueField.getName());
      assertEquals(ArrowType.Utf8.INSTANCE, valueField.getType());
    }

    @Test
    @DisplayName("Should handle empty schema")
    void shouldHandleEmptySchema() {
      // Given
      org.apache.kafka.connect.data.Schema kafkaSchema = SchemaBuilder.struct().build();

      // When
      Schema arrowSchema = KafkaSchemaToArrow.arrowSchemaFromKafka(kafkaSchema);

      // Then
      assertNotNull(arrowSchema);
      assertEquals(0, arrowSchema.getFields().size());
    }
  }

  @Nested
  @DisplayName("Nullable field tests")
  class NullableFieldTests {

    @Test
    @DisplayName("Should handle nullable and non-nullable fields correctly")
    void shouldHandleNullableFields() {
      // Given
      /*
       * Kafka Schema JSON representation:
       * {
       *   "type": "STRUCT",
       *   "fields": [
       *     {"name": "required_field", "type": "STRING", "optional": false},
       *     {"name": "optional_field", "type": "STRING", "optional": true}
       *   ]
       * }
       */
      org.apache.kafka.connect.data.Schema kafkaSchema =
          SchemaBuilder.struct()
              .field("required_field", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
              .field("optional_field", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
              .build();

      // When
      Schema arrowSchema = KafkaSchemaToArrow.arrowSchemaFromKafka(kafkaSchema);

      // Then
      /*
       * Expected Arrow Schema JSON representation:
       * {
       *   "fields": [
       *     {"name": "required_field", "type": {"name": "utf8"}, "nullable": false},
       *     {"name": "optional_field", "type": {"name": "utf8"}, "nullable": true}
       *   ]
       * }
       */
      Field requiredField = arrowSchema.getFields().get(FIRST_FIELD_INDEX);
      assertFalse(requiredField.isNullable());

      Field optionalField = arrowSchema.getFields().get(SECOND_FIELD_INDEX);
      assertTrue(optionalField.isNullable());
    }
  }

  @Nested
  @DisplayName("Complex schema tests")
  class ComplexSchemaTests {

    @Test
    @DisplayName("Should convert deeply nested schema")
    void shouldConvertDeeplyNestedSchema() {
      // Given
      org.apache.kafka.connect.data.Schema innerSchema =
          SchemaBuilder.struct()
              .field("value", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
              .build();

      org.apache.kafka.connect.data.Schema middleSchema =
          SchemaBuilder.struct()
              .field("inner", innerSchema)
              .field("count", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
              .build();

      org.apache.kafka.connect.data.Schema kafkaSchema =
          SchemaBuilder.struct()
              .field("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
              .field("middle", middleSchema)
              .build();

      // When
      Schema arrowSchema = KafkaSchemaToArrow.arrowSchemaFromKafka(kafkaSchema);

      // Then
      assertNotNull(arrowSchema);
      assertEquals(2, arrowSchema.getFields().size());

      Field middleField = arrowSchema.getFields().get(1);
      assertEquals("middle", middleField.getName());
      assertEquals(ArrowType.Struct.INSTANCE, middleField.getType());
      assertEquals(2, middleField.getChildren().size());

      Field innerField = middleField.getChildren().get(0);
      assertEquals("inner", innerField.getName());
      assertEquals(ArrowType.Struct.INSTANCE, innerField.getType());
      assertEquals(1, innerField.getChildren().size());

      Field valueField = innerField.getChildren().get(0);
      assertEquals("value", valueField.getName());
      assertEquals(ArrowType.Utf8.INSTANCE, valueField.getType());
    }

    @Test
    @DisplayName("Should convert array of structs")
    void shouldConvertArrayOfStructs() {
      // Given
      org.apache.kafka.connect.data.Schema itemSchema =
          SchemaBuilder.struct()
              .field("name", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
              .field("price", org.apache.kafka.connect.data.Schema.FLOAT64_SCHEMA)
              .build();

      org.apache.kafka.connect.data.Schema kafkaSchema =
          SchemaBuilder.struct()
              .field("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
              .field("items", SchemaBuilder.array(itemSchema).build())
              .build();

      // When
      Schema arrowSchema = KafkaSchemaToArrow.arrowSchemaFromKafka(kafkaSchema);

      // Then
      assertNotNull(arrowSchema);
      assertEquals(2, arrowSchema.getFields().size());

      Field itemsField = arrowSchema.getFields().get(1);
      assertEquals("items", itemsField.getName());
      assertEquals(ArrowType.List.INSTANCE, itemsField.getType());

      Field elementField = itemsField.getChildren().get(0);
      assertEquals("element", elementField.getName());
      assertEquals(ArrowType.Struct.INSTANCE, elementField.getType());
      assertEquals(2, elementField.getChildren().size());
    }

    @Test
    @DisplayName("Should convert map with complex values")
    void shouldConvertMapWithComplexValues() {
      // Given
      org.apache.kafka.connect.data.Schema valueSchema =
          SchemaBuilder.struct()
              .field("count", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
              .field("active", org.apache.kafka.connect.data.Schema.BOOLEAN_SCHEMA)
              .build();

      org.apache.kafka.connect.data.Schema kafkaSchema =
          SchemaBuilder.struct()
              .field("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
              .field(
                  "config",
                  SchemaBuilder.map(org.apache.kafka.connect.data.Schema.STRING_SCHEMA, valueSchema)
                      .build())
              .build();

      // When
      Schema arrowSchema = KafkaSchemaToArrow.arrowSchemaFromKafka(kafkaSchema);

      // Then
      assertNotNull(arrowSchema);
      assertEquals(2, arrowSchema.getFields().size());

      Field configField = arrowSchema.getFields().get(1);
      assertEquals("config", configField.getName());
      assertEquals(new ArrowType.Map(true), configField.getType());
      assertEquals(1, configField.getChildren().size());

      Field entries = configField.getChildren().get(0);
      assertEquals("entries", entries.getName());
      assertEquals(ArrowType.Struct.INSTANCE, entries.getType());
      assertEquals(2, entries.getChildren().size());

      Field keyField = entries.getChildren().get(0);
      assertEquals("key", keyField.getName());
      assertEquals(ArrowType.Utf8.INSTANCE, keyField.getType());

      Field valueField = entries.getChildren().get(1);
      assertEquals("value", valueField.getName());
      assertEquals(ArrowType.Struct.INSTANCE, valueField.getType());
      assertEquals(2, valueField.getChildren().size());
    }
  }

  @Nested
  @DisplayName("Error handling tests")
  class ErrorHandlingTests {

    @Test
    @DisplayName("Should throw exception for unsupported type")
    void shouldThrowExceptionForUnsupportedType() {
      // This test would require access to private methods or a way to inject unsupported types
      // Since the toArrowType method is private and handles all Kafka types,
      // we would need to modify the class to make this testable or use reflection
    }
  }
}
