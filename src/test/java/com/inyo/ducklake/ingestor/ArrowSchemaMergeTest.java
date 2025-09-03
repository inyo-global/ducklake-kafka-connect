package com.inyo.ducklake.ingestor;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.*;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

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
            Schema unified = ArrowSchemaMerge.unifySchemas(Arrays.asList(schema1, schema2));

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
            Schema unified = ArrowSchemaMerge.unifySchemas(Arrays.asList(schema1, schema2));

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
            Schema unified = ArrowSchemaMerge.unifySchemas(Arrays.asList(schema1, schema2));

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
            Schema unified = ArrowSchemaMerge.unifySchemas(Collections.singletonList(schema));

            // Then
            assertEquals(schema, unified);
        }

        @Test
        @DisplayName("Should throw exception for null or empty schema list")
        void shouldThrowExceptionForNullOrEmptySchemaList() {
            // Then
            assertThrows(IllegalArgumentException.class,
                () -> ArrowSchemaMerge.unifySchemas(null));
            assertThrows(IllegalArgumentException.class,
                () -> ArrowSchemaMerge.unifySchemas(Collections.emptyList()));
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
            Schema unified = ArrowSchemaMerge.unifySchemas(Arrays.asList(schema1, schema2, schema3));

            // Then
            Field unifiedField = unified.getFields().get(0);
            assertEquals(new ArrowType.Int(64, true), unifiedField.getFieldType().getType());
        }

        @Test
        @DisplayName("Should promote to floating point when mixed with integers")
        void shouldPromoteToFloatingPointWhenMixedWithIntegers() {
            // Given
            Field field1 = createField("value", new ArrowType.Int(32, true), false);
            Field field2 = createField("value", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), false);

            Schema schema1 = new Schema(Collections.singletonList(field1));
            Schema schema2 = new Schema(Collections.singletonList(field2));

            // When
            Schema unified = ArrowSchemaMerge.unifySchemas(Arrays.asList(schema1, schema2));

            // Then
            Field unifiedField = unified.getFields().get(0);
            assertEquals(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE),
                unifiedField.getFieldType().getType());
        }

        @Test
        @DisplayName("Should promote to double precision floating point")
        void shouldPromoteToDoublePrecisionFloatingPoint() {
            // Given
            Field field1 = createField("value", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), false);
            Field field2 = createField("value", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), false);

            Schema schema1 = new Schema(Collections.singletonList(field1));
            Schema schema2 = new Schema(Collections.singletonList(field2));

            // When
            Schema unified = ArrowSchemaMerge.unifySchemas(Arrays.asList(schema1, schema2));

            // Then
            Field unifiedField = unified.getFields().get(0);
            assertEquals(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
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
            assertThrows(IllegalArgumentException.class,
                () -> ArrowSchemaMerge.unifySchemas(Arrays.asList(schema1, schema2)));
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
            Schema unified = ArrowSchemaMerge.unifySchemas(Arrays.asList(schema1, schema2));

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
            Schema unified = ArrowSchemaMerge.unifySchemas(Arrays.asList(schema1, schema2));

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
            Schema unified = ArrowSchemaMerge.unifySchemas(Arrays.asList(schema1, schema2));

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
            assertTrue(ArrowSchemaMerge.areCompatible(schema1, schema2)); // Int32 and Int64 are compatible
            assertFalse(ArrowSchemaMerge.areCompatible(schema1, schema3)); // Int32 and String are not compatible
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
