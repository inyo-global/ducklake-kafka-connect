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
package com.inyo.ducklake.ingestor;

import static com.inyo.ducklake.ingestor.ArrowSchemaMerge.unifySchemas;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ArrowSchemaMergeTest {

  @Nested
  @DisplayName("Schema Unification Tests")
  class SchemaUnificationTests {

    @Test
    @DisplayName("Should unify schemas with same fields")
    void shouldUnifySchemasWithSameFields() {
      // Given
      Field field1 = createField("id", new ArrowType.Int(32, true), false);
      Field field2 = createField("name", ArrowType.Utf8.INSTANCE, false);

      Schema schema1 = new Schema(Arrays.asList(field1, field2));
      Schema schema2 = new Schema(Arrays.asList(field1, field2));

      // When
      Schema unified = ArrowSchemaMerge.unifySchemas(Arrays.asList(schema1, schema2), HashMap::new);

      // Then
      assertEquals(2, unified.getFields().size());
      assertEquals("id", unified.getFields().get(0).getName());
      assertEquals("name", unified.getFields().get(1).getName());
    }

    @Test
    @DisplayName("Should unify schemas with different fields")
    void shouldUnifySchemasWithDifferentFields() {
      // Given
      Field field1 = createField("id", new ArrowType.Int(32, true), false);
      Field field2 = createField("name", ArrowType.Utf8.INSTANCE, false);
      Field field3 = createField("age", new ArrowType.Int(32, true), false);

      Schema schema1 = new Schema(Arrays.asList(field1, field2));
      Schema schema2 = new Schema(Arrays.asList(field1, field3));

      // When
      Schema unified =
          ArrowSchemaMerge.unifySchemas(Arrays.asList(schema1, schema2), () -> new HashMap<>());

      // Then
      assertEquals(3, unified.getFields().size());
      assertTrue(unified.getFields().stream().anyMatch(f -> f.getName().equals("id")));
      assertTrue(unified.getFields().stream().anyMatch(f -> f.getName().equals("name")));
      assertTrue(unified.getFields().stream().anyMatch(f -> f.getName().equals("age")));
    }

    @Test
    @DisplayName("Should handle nullable field merging")
    void shouldHandleNullableFieldMerging() {
      // Given
      Field field1 = createField("name", ArrowType.Utf8.INSTANCE, false);
      Field field2 = createField("name", ArrowType.Utf8.INSTANCE, true);

      Schema schema1 = new Schema(Collections.singletonList(field1));
      Schema schema2 = new Schema(Collections.singletonList(field2));

      // When
      Schema unified =
          ArrowSchemaMerge.unifySchemas(Arrays.asList(schema1, schema2), () -> new HashMap<>());

      // Then
      assertEquals(1, unified.getFields().size());
      Field unifiedField = unified.getFields().get(0);
      assertEquals("name", unifiedField.getName());
      assertTrue(unifiedField.isNullable()); // Should be nullable if any source is nullable
    }

    @Test
    @DisplayName("Should return same schema for single input")
    void shouldReturnSameSchemaForSingleInput() {
      // Given
      Field field = createField("id", new ArrowType.Int(32, true), false);
      Schema schema = new Schema(Collections.singletonList(field));

      // When
      Schema unified =
          ArrowSchemaMerge.unifySchemas(Collections.singletonList(schema), HashMap::new);

      // Then
      assertEquals(schema, unified);
    }

    @Test
    @DisplayName("Should throw exception for null or empty schema list")
    void shouldThrowExceptionForNullOrEmptySchemaList() {
      // Then
      assertThrows(
          IllegalArgumentException.class, () -> ArrowSchemaMerge.unifySchemas(null, HashMap::new));
      assertThrows(
          IllegalArgumentException.class,
          () -> ArrowSchemaMerge.unifySchemas(Collections.emptyList(), HashMap::new));
    }
  }

  @Nested
  @DisplayName("Type Unification Tests")
  class TypeUnificationTests {

    @Test
    @DisplayName("Should promote integer types")
    void shouldPromoteIntegerTypes() {
      // Given
      Field field1 = createField("value", new ArrowType.Int(16, true), false);
      Field field2 = createField("value", new ArrowType.Int(32, true), false);
      Field field3 = createField("value", new ArrowType.Int(64, true), false);

      Schema schema1 = new Schema(Collections.singletonList(field1));
      Schema schema2 = new Schema(Collections.singletonList(field2));
      Schema schema3 = new Schema(Collections.singletonList(field3));

      // When
      Schema unified =
          ArrowSchemaMerge.unifySchemas(
              Arrays.asList(schema1, schema2, schema3), () -> new HashMap<>());

      // Then
      Field unifiedField = unified.getFields().get(0);
      assertEquals(new ArrowType.Int(64, true), unifiedField.getFieldType().getType());
    }

    @Test
    @DisplayName("Should promote to floating point when mixed with integers")
    void shouldPromoteToFloatingPointWhenMixedWithIntegers() {
      // Given
      Field field1 = createField("value", new ArrowType.Int(32, true), false);
      Field field2 =
          createField("value", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), false);

      Schema schema1 = new Schema(Collections.singletonList(field1));
      Schema schema2 = new Schema(Collections.singletonList(field2));

      // When
      Schema unified = ArrowSchemaMerge.unifySchemas(Arrays.asList(schema1, schema2), HashMap::new);

      // Then
      Field unifiedField = unified.getFields().get(0);
      assertEquals(
          new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE),
          unifiedField.getFieldType().getType());
    }

    @Test
    @DisplayName("Should promote to double precision floating point")
    void shouldPromoteToDoublePrecisionFloatingPoint() {
      // Given
      Field field1 =
          createField("value", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), false);
      Field field2 =
          createField("value", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), false);

      Schema schema1 = new Schema(Collections.singletonList(field1));
      Schema schema2 = new Schema(Collections.singletonList(field2));

      // When
      Schema unified = ArrowSchemaMerge.unifySchemas(Arrays.asList(schema1, schema2), HashMap::new);

      // Then
      Field unifiedField = unified.getFields().get(0);
      assertEquals(
          new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
          unifiedField.getFieldType().getType());
    }

    @Test
    @DisplayName("Should throw exception for incompatible types")
    void shouldThrowExceptionForIncompatibleTypes() {
      // Given
      Field field1 = createField("value", ArrowType.Utf8.INSTANCE, false);
      Field field2 = createField("value", new ArrowType.Int(32, true), false);

      Schema schema1 = new Schema(Collections.singletonList(field1));
      Schema schema2 = new Schema(Collections.singletonList(field2));

      // Then
      assertThrows(
          IllegalArgumentException.class,
          () -> ArrowSchemaMerge.unifySchemas(Arrays.asList(schema1, schema2), HashMap::new));
    }
  }

  @Nested
  @DisplayName("Complex Type Tests")
  class ComplexTypeTests {

    @Test
    @DisplayName("Should merge struct fields")
    void shouldMergeStructFields() {
      // Given
      Field innerField1 = createField("street", ArrowType.Utf8.INSTANCE, false);
      Field innerField2 = createField("city", ArrowType.Utf8.INSTANCE, false);
      Field innerField3 = createField("zipcode", ArrowType.Utf8.INSTANCE, false);

      Field struct1 = createStructField("address", Arrays.asList(innerField1, innerField2));
      Field struct2 = createStructField("address", Arrays.asList(innerField1, innerField3));

      Schema schema1 = new Schema(Collections.singletonList(struct1));
      Schema schema2 = new Schema(Collections.singletonList(struct2));

      // When
      Schema unified = ArrowSchemaMerge.unifySchemas(Arrays.asList(schema1, schema2), HashMap::new);

      // Then
      assertEquals(1, unified.getFields().size());
      Field unifiedStruct = unified.getFields().get(0);
      assertEquals("address", unifiedStruct.getName());
      assertEquals(ArrowType.Struct.INSTANCE, unifiedStruct.getFieldType().getType());
      assertEquals(3, unifiedStruct.getChildren().size()); // street, city, zipcode
    }

    @Test
    @DisplayName("Should merge list fields")
    void shouldMergeListFields() {
      // Given
      Field elementField = createField("element", ArrowType.Utf8.INSTANCE, false);
      Field list1 = createListField("tags", elementField);
      Field list2 = createListField("tags", elementField);

      Schema schema1 = new Schema(Collections.singletonList(list1));
      Schema schema2 = new Schema(Collections.singletonList(list2));

      // When
      Schema unified =
          ArrowSchemaMerge.unifySchemas(Arrays.asList(schema1, schema2), () -> new HashMap<>());

      // Then
      assertEquals(1, unified.getFields().size());
      Field unifiedList = unified.getFields().get(0);
      assertEquals("tags", unifiedList.getName());
      assertEquals(ArrowType.List.INSTANCE, unifiedList.getFieldType().getType());
    }

    @Test
    @DisplayName("Should merge map fields")
    void shouldMergeMapFields() {
      // Given
      Field keyField = createField("key", ArrowType.Utf8.INSTANCE, false);
      Field valueField = createField("value", ArrowType.Utf8.INSTANCE, false);
      Field map1 = createMapField("metadata", keyField, valueField);
      Field map2 = createMapField("metadata", keyField, valueField);

      Schema schema1 = new Schema(Collections.singletonList(map1));
      Schema schema2 = new Schema(Collections.singletonList(map2));

      // When
      Schema unified = ArrowSchemaMerge.unifySchemas(Arrays.asList(schema1, schema2), HashMap::new);

      // Then
      assertEquals(1, unified.getFields().size());
      Field unifiedMap = unified.getFields().get(0);
      assertEquals("metadata", unifiedMap.getName());
      assertEquals(new ArrowType.Map(true), unifiedMap.getFieldType().getType());
    }
  }

  @Nested
  @DisplayName("Utility Method Tests")
  class UtilityMethodTests {

    @Test
    @DisplayName("Should check schema compatibility")
    void shouldCheckSchemaCompatibility() {
      // Given
      Field field1 = createField("id", new ArrowType.Int(32, true), false);
      Field field2 = createField("id", new ArrowType.Int(64, true), false);
      Field field3 = createField("id", ArrowType.Utf8.INSTANCE, false);

      Schema schema1 = new Schema(Collections.singletonList(field1));
      Schema schema2 = new Schema(Collections.singletonList(field2));
      Schema schema3 = new Schema(Collections.singletonList(field3));

      // Then
      // Int32 and Int64 are compatible
      assertDoesNotThrow(
          () -> ArrowSchemaMerge.unifySchemas(Arrays.asList(schema1, schema2), HashMap::new));

      // Int32 and String are not compatible
      assertThrows(
          IllegalArgumentException.class,
          () -> ArrowSchemaMerge.unifySchemas(Arrays.asList(schema1, schema3), HashMap::new));
    }

    @Test
    @DisplayName("Should add field to schema")
    void shouldAddFieldToSchema() {
      // Given
      Field existingField = createField("id", new ArrowType.Int(32, true), false);
      Field newField = createField("name", ArrowType.Utf8.INSTANCE, false);
      Schema schema = new Schema(Collections.singletonList(existingField));

      // When
      Schema updatedSchema = ArrowSchemaMerge.addField(schema, newField);

      // Then
      assertEquals(2, updatedSchema.getFields().size());
      assertTrue(updatedSchema.getFields().stream().anyMatch(f -> f.getName().equals("id")));
      assertTrue(updatedSchema.getFields().stream().anyMatch(f -> f.getName().equals("name")));
    }

    @Test
    @DisplayName("Should merge when adding existing field")
    void shouldMergeWhenAddingExistingField() {
      // Given
      Field existingField = createField("name", ArrowType.Utf8.INSTANCE, false);
      Field newField = createField("name", ArrowType.Utf8.INSTANCE, true); // nullable version
      Schema schema = new Schema(Collections.singletonList(existingField));

      // When
      Schema updatedSchema = ArrowSchemaMerge.addField(schema, newField);

      // Then
      assertEquals(1, updatedSchema.getFields().size());
      Field mergedField = updatedSchema.getFields().get(0);
      assertEquals("name", mergedField.getName());
      assertTrue(mergedField.isNullable()); // Should be nullable after merge
    }

    @Test
    @DisplayName("Should remove field from schema")
    void shouldRemoveFieldFromSchema() {
      // Given
      Field field1 = createField("id", new ArrowType.Int(32, true), false);
      Field field2 = createField("name", ArrowType.Utf8.INSTANCE, false);
      Schema schema = new Schema(Arrays.asList(field1, field2));

      // When
      Schema updatedSchema = ArrowSchemaMerge.removeField(schema, "name");

      // Then
      assertEquals(1, updatedSchema.getFields().size());
      assertEquals("id", updatedSchema.getFields().get(0).getName());
    }
  }

  @Nested
  @DisplayName("String-Timestamp Type Mismatch Tests")
  class StringTimestampTypeMismatchTests {

    @Test
    @DisplayName("Should throw error when mixing string and timestamp types")
    void shouldThrowErrorWhenMixingStringAndTimestampTypes() {
      // Given - one schema has a field as string, another has it as timestamp
      // This simulates the distinct_id bug where some records incorrectly inferred timestamp
      Field stringField = createField("distinct_id", ArrowType.Utf8.INSTANCE, false);
      Field timestampField =
          createField(
              "distinct_id",
              new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, null),
              false);

      Schema schema1 = new Schema(Collections.singletonList(stringField));
      Schema schema2 = new Schema(Collections.singletonList(timestampField));

      // When/Then - should throw error, not silently unify
      // This allows the error to be caught at record level and routed to DLQ
      assertThrows(
          IllegalArgumentException.class,
          () -> ArrowSchemaMerge.unifySchemas(Arrays.asList(schema1, schema2), HashMap::new));
    }

    @Test
    @DisplayName("Should throw error when mixing string and date types")
    void shouldThrowErrorWhenMixingStringAndDateTypes() {
      // Given
      Field stringField = createField("created_date", ArrowType.Utf8.INSTANCE, false);
      Field dateField =
          createField(
              "created_date",
              new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY),
              false);

      Schema schema1 = new Schema(Collections.singletonList(stringField));
      Schema schema2 = new Schema(Collections.singletonList(dateField));

      // When/Then - should throw error
      assertThrows(
          IllegalArgumentException.class,
          () -> ArrowSchemaMerge.unifySchemas(Arrays.asList(schema1, schema2), HashMap::new));
    }

    @Test
    @DisplayName("Should throw error when mixing string and time types")
    void shouldThrowErrorWhenMixingStringAndTimeTypes() {
      // Given
      Field stringField = createField("event_time", ArrowType.Utf8.INSTANCE, false);
      Field timeField =
          createField(
              "event_time",
              new ArrowType.Time(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, 32),
              false);

      Schema schema1 = new Schema(Collections.singletonList(stringField));
      Schema schema2 = new Schema(Collections.singletonList(timeField));

      // When/Then - should throw error
      assertThrows(
          IllegalArgumentException.class,
          () -> ArrowSchemaMerge.unifySchemas(Arrays.asList(schema1, schema2), HashMap::new));
    }

    @Test
    @DisplayName("Should still unify pure timestamp types correctly")
    void shouldStillUnifyPureTimestampTypesCorrectly() {
      // Given - both schemas have timestamp, should still unify to timestamp
      Field timestampField1 =
          createField(
              "created_at",
              new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, null),
              false);
      Field timestampField2 =
          createField(
              "created_at",
              new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, null),
              false);

      Schema schema1 = new Schema(Collections.singletonList(timestampField1));
      Schema schema2 = new Schema(Collections.singletonList(timestampField2));

      // When
      Schema unified = ArrowSchemaMerge.unifySchemas(Arrays.asList(schema1, schema2), HashMap::new);

      // Then - should unify to timestamp
      assertEquals(1, unified.getFields().size());
      Field unifiedField = unified.getFields().get(0);
      assertInstanceOf(ArrowType.Timestamp.class, unifiedField.getFieldType().getType());
    }

    @Test
    @DisplayName("Error message should include field name and conflicting types")
    void errorMessageShouldIncludeFieldNameAndConflictingTypes() {
      // Given
      Field stringField = createField("distinct_id", ArrowType.Utf8.INSTANCE, false);
      Field timestampField =
          createField(
              "distinct_id",
              new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, null),
              false);

      Schema schema1 = new Schema(Collections.singletonList(stringField));
      Schema schema2 = new Schema(Collections.singletonList(timestampField));

      // When/Then
      var exception =
          assertThrows(
              IllegalArgumentException.class,
              () -> ArrowSchemaMerge.unifySchemas(Arrays.asList(schema1, schema2), HashMap::new));

      // Error message should help identify the problem
      assertTrue(exception.getMessage().contains("distinct_id"));
      assertTrue(
          exception.getMessage().contains("Timestamp") || exception.getMessage().contains("Utf8"));
    }
  }

  @Nested
  @DisplayName("Enhanced Error Messaging Tests")
  class EnhancedErrorMessagingTests {

    @Test
    @DisplayName("Should provide sample values in error message when unifying incompatible types")
    void shouldProvideSampleValuesInErrorMessage() {
      // Given
      Field structField =
          createStructField(
              "address",
              Arrays.asList(
                  createField("street", ArrowType.Utf8.INSTANCE, false),
                  createField("city", ArrowType.Utf8.INSTANCE, false)));
      Field stringField = createField("address", ArrowType.Utf8.INSTANCE, false);

      Schema schema1 = new Schema(Collections.singletonList(structField));
      Schema schema2 = new Schema(Collections.singletonList(stringField));

      var sampleValues =
          Map.of(
              "address",
              Arrays.asList(Map.of("street", "123 Main St", "city", "New York"), "456 Oak Avenue"));

      // When & Then
      var exception =
          assertThrows(
              IllegalArgumentException.class,
              () -> unifySchemas(Arrays.asList(schema1, schema2), () -> sampleValues));

      assertTrue(exception.getMessage().contains("Sample values:"));
      assertTrue(exception.getMessage().contains("123 Main St"));
      assertTrue(exception.getMessage().contains("456 Oak Avenue"));
      assertTrue(exception.getMessage().contains("address"));
    }

    @Test
    @DisplayName("Should handle null sample values gracefully")
    void shouldHandleNullSampleValuesGracefully() {
      // Given
      Field intField = createField("id", new ArrowType.Int(32, true), false);
      Field stringField = createField("id", ArrowType.Utf8.INSTANCE, false);

      Schema schema1 = new Schema(Collections.singletonList(intField));
      Schema schema2 = new Schema(Collections.singletonList(stringField));

      Map<String, List<Object>> sampleValues =
          Map.of("id", Arrays.asList((Object) 42, (Object) "string-id"));

      // When & Then
      var exception =
          assertThrows(
              IllegalArgumentException.class,
              () -> unifySchemas(Arrays.asList(schema1, schema2), () -> sampleValues));

      assertTrue(exception.getMessage().contains("Sample values:"));
      assertTrue(exception.getMessage().contains("42"));
      assertTrue(exception.getMessage().contains("string-id"));
    }

    @Test
    @DisplayName("Should handle empty sample values map")
    void shouldHandleEmptySampleValuesMap() {
      // Given
      Field intField = createField("count", new ArrowType.Int(32, true), false);
      Field stringField = createField("count", ArrowType.Utf8.INSTANCE, false);

      Schema schema1 = new Schema(Collections.singletonList(intField));
      Schema schema2 = new Schema(Collections.singletonList(stringField));

      Map<String, List<Object>> emptySampleValues = Map.of();

      // When & Then
      var exception =
          assertThrows(
              IllegalArgumentException.class,
              () -> unifySchemas(Arrays.asList(schema1, schema2), () -> emptySampleValues));

      assertTrue(exception.getMessage().contains("Sample values: null"));
    }

    @Test
    @DisplayName("Should work with compatible schemas using sample values")
    void shouldWorkWithCompatibleSchemasUsingSampleValues() {
      // Given
      Field int32Field = createField("number", new ArrowType.Int(32, true), false);
      Field int64Field = createField("number", new ArrowType.Int(64, true), false);

      Schema schema1 = new Schema(Collections.singletonList(int32Field));
      Schema schema2 = new Schema(Collections.singletonList(int64Field));

      Map<String, List<Object>> sampleValues = Map.of("number", Arrays.asList(42, 123456789L));

      // When
      Schema unified = unifySchemas(Arrays.asList(schema1, schema2), () -> sampleValues);

      // Then
      assertEquals(1, unified.getFields().size());
      assertEquals("number", unified.getFields().get(0).getName());
      assertInstanceOf(ArrowType.Int.class, unified.getFields().get(0).getType());
      assertEquals(64, ((ArrowType.Int) unified.getFields().get(0).getType()).getBitWidth());
    }
  }

  // Helper methods

  private Field createField(String name, ArrowType type, boolean nullable) {
    FieldType fieldType = new FieldType(nullable, type, null);
    return new Field(name, fieldType, null);
  }

  private Field createStructField(String name, List<Field> children) {
    FieldType fieldType = new FieldType(false, ArrowType.Struct.INSTANCE, null);
    return new Field(name, fieldType, children);
  }

  private Field createListField(String name, Field elementField) {
    FieldType fieldType = new FieldType(false, ArrowType.List.INSTANCE, null);
    return new Field(name, fieldType, Collections.singletonList(elementField));
  }

  private Field createMapField(String name, Field keyField, Field valueField) {
    FieldType fieldType = new FieldType(false, new ArrowType.Map(true), null);
    return new Field(name, fieldType, Arrays.asList(keyField, valueField));
  }
}
