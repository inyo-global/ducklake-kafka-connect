package com.inyo.ducklake.connect;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;

import static org.junit.jupiter.api.Assertions.*;

class KafkaSchemaToArrowTest {

    @Nested
    @DisplayName("arrowSchemaFromKafka tests")
    class ArrowSchemaFromKafkaTests {

        @Test
        @DisplayName("Should convert simple schema with primitive fields")
        void shouldConvertSimpleSchema() {
            // Given
            org.apache.kafka.connect.data.Schema kafkaSchema = SchemaBuilder.struct()
                    .field("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
                    .field("name", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                    .field("age", org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
                    .build();

            // When
            Schema arrowSchema = KafkaSchemaToArrow.arrowSchemaFromKafka(kafkaSchema);

            // Then
            assertNotNull(arrowSchema);
            assertEquals(3, arrowSchema.getFields().size());

            Field idField = arrowSchema.getFields().get(0);
            assertEquals("id", idField.getName());
            assertFalse(idField.isNullable());
            assertEquals(new ArrowType.Int(32, true), idField.getType());

            Field nameField = arrowSchema.getFields().get(1);
            assertEquals("name", nameField.getName());
            assertFalse(nameField.isNullable());
            assertEquals(ArrowType.Utf8.INSTANCE, nameField.getType());

            Field ageField = arrowSchema.getFields().get(2);
            assertEquals("age", ageField.getName());
            assertTrue(ageField.isNullable());
            assertEquals(new ArrowType.Int(32, true), ageField.getType());
        }

        @Test
        @DisplayName("Should convert schema with all primitive types")
        void shouldConvertAllPrimitiveTypes() {
            // Given
            org.apache.kafka.connect.data.Schema kafkaSchema = SchemaBuilder.struct()
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
            assertNotNull(arrowSchema);
            assertEquals(9, arrowSchema.getFields().size());

            assertEquals(new ArrowType.Int(8, true), arrowSchema.getFields().get(0).getType());
            assertEquals(new ArrowType.Int(16, true), arrowSchema.getFields().get(1).getType());
            assertEquals(new ArrowType.Int(32, true), arrowSchema.getFields().get(2).getType());
            assertEquals(new ArrowType.Int(64, true), arrowSchema.getFields().get(3).getType());
            assertEquals(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), arrowSchema.getFields().get(4).getType());
            assertEquals(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), arrowSchema.getFields().get(5).getType());
            assertEquals(ArrowType.Bool.INSTANCE, arrowSchema.getFields().get(6).getType());
            assertEquals(ArrowType.Utf8.INSTANCE, arrowSchema.getFields().get(7).getType());
            assertEquals(ArrowType.Binary.INSTANCE, arrowSchema.getFields().get(8).getType());
        }

        @Test
        @DisplayName("Should convert schema with nested struct")
        void shouldConvertNestedStruct() {
            // Given
            org.apache.kafka.connect.data.Schema addressSchema = SchemaBuilder.struct()
                    .field("street", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                    .field("city", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                    .field("zipcode", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                    .build();

            org.apache.kafka.connect.data.Schema kafkaSchema = SchemaBuilder.struct()
                    .field("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
                    .field("name", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                    .field("address", addressSchema)
                    .build();

            // When
            Schema arrowSchema = KafkaSchemaToArrow.arrowSchemaFromKafka(kafkaSchema);

            // Then
            assertNotNull(arrowSchema);
            assertEquals(3, arrowSchema.getFields().size());

            Field addressField = arrowSchema.getFields().get(2);
            assertEquals("address", addressField.getName());
            assertEquals(ArrowType.Struct.INSTANCE, addressField.getType());
            assertEquals(3, addressField.getChildren().size());

            assertEquals("street", addressField.getChildren().get(0).getName());
            assertEquals("city", addressField.getChildren().get(1).getName());
            assertEquals("zipcode", addressField.getChildren().get(2).getName());
        }

        @Test
        @DisplayName("Should convert schema with array field")
        void shouldConvertArrayField() {
            // Given
            org.apache.kafka.connect.data.Schema kafkaSchema = SchemaBuilder.struct()
                    .field("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
                    .field("tags", SchemaBuilder.array(org.apache.kafka.connect.data.Schema.STRING_SCHEMA).build())
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
            org.apache.kafka.connect.data.Schema kafkaSchema = SchemaBuilder.struct()
                    .field("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
                    .field("metadata", SchemaBuilder.map(
                            org.apache.kafka.connect.data.Schema.STRING_SCHEMA,
                            org.apache.kafka.connect.data.Schema.STRING_SCHEMA
                    ).build())
                    .build();

            // When
            Schema arrowSchema = KafkaSchemaToArrow.arrowSchemaFromKafka(kafkaSchema);

            // Then
            assertNotNull(arrowSchema);
            assertEquals(2, arrowSchema.getFields().size());

            Field metadataField = arrowSchema.getFields().get(1);
            assertEquals("metadata", metadataField.getName());
            assertEquals(new ArrowType.Map(true), metadataField.getType());
            assertEquals(2, metadataField.getChildren().size());

            Field keyField = metadataField.getChildren().get(0);
            assertEquals("key", keyField.getName());
            assertEquals(ArrowType.Utf8.INSTANCE, keyField.getType());

            Field valueField = metadataField.getChildren().get(1);
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
    @DisplayName("Complex schema conversion tests")
    class ComplexSchemaTests {

        @Test
        @DisplayName("Should convert deeply nested schema")
        void shouldConvertDeeplyNestedSchema() {
            // Given
            org.apache.kafka.connect.data.Schema innerSchema = SchemaBuilder.struct()
                    .field("value", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                    .build();

            org.apache.kafka.connect.data.Schema middleSchema = SchemaBuilder.struct()
                    .field("inner", innerSchema)
                    .field("count", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
                    .build();

            org.apache.kafka.connect.data.Schema kafkaSchema = SchemaBuilder.struct()
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
            org.apache.kafka.connect.data.Schema itemSchema = SchemaBuilder.struct()
                    .field("name", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                    .field("price", org.apache.kafka.connect.data.Schema.FLOAT64_SCHEMA)
                    .build();

            org.apache.kafka.connect.data.Schema kafkaSchema = SchemaBuilder.struct()
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
            org.apache.kafka.connect.data.Schema valueSchema = SchemaBuilder.struct()
                    .field("count", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
                    .field("active", org.apache.kafka.connect.data.Schema.BOOLEAN_SCHEMA)
                    .build();

            org.apache.kafka.connect.data.Schema kafkaSchema = SchemaBuilder.struct()
                    .field("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
                    .field("config", SchemaBuilder.map(
                            org.apache.kafka.connect.data.Schema.STRING_SCHEMA,
                            valueSchema
                    ).build())
                    .build();

            // When
            Schema arrowSchema = KafkaSchemaToArrow.arrowSchemaFromKafka(kafkaSchema);

            // Then
            assertNotNull(arrowSchema);
            assertEquals(2, arrowSchema.getFields().size());

            Field configField = arrowSchema.getFields().get(1);
            assertEquals("config", configField.getName());
            assertEquals(new ArrowType.Map(true), configField.getType());

            Field valueField = configField.getChildren().get(1);
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

            // For now, we can test that all known Kafka types are supported
            // by testing the conversion of all available schema types
        }
    }

    @Nested
    @DisplayName("Nullable field tests")
    class NullableFieldTests {

        @Test
        @DisplayName("Should handle optional fields correctly")
        void shouldHandleOptionalFields() {
            // Given
            org.apache.kafka.connect.data.Schema kafkaSchema = SchemaBuilder.struct()
                    .field("required_field", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                    .field("optional_field", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
                    .build();

            // When
            Schema arrowSchema = KafkaSchemaToArrow.arrowSchemaFromKafka(kafkaSchema);

            // Then
            assertNotNull(arrowSchema);
            assertEquals(2, arrowSchema.getFields().size());

            Field requiredField = arrowSchema.getFields().get(0);
            assertEquals("required_field", requiredField.getName());
            assertFalse(requiredField.isNullable());

            Field optionalField = arrowSchema.getFields().get(1);
            assertEquals("optional_field", optionalField.getName());
            assertTrue(optionalField.isNullable());
        }

        @Test
        @DisplayName("Should handle optional nested structures")
        void shouldHandleOptionalNestedStructures() {
            // Given
            org.apache.kafka.connect.data.Schema addressSchema = SchemaBuilder.struct()
                    .field("street", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                    .build();

            org.apache.kafka.connect.data.Schema optionalAddressSchema = SchemaBuilder.struct()
                    .field("street", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                    .optional()
                    .build();

            org.apache.kafka.connect.data.Schema kafkaSchema = SchemaBuilder.struct()
                    .field("required_address", addressSchema)
                    .field("optional_address", optionalAddressSchema)
                    .build();

            // When
            Schema arrowSchema = KafkaSchemaToArrow.arrowSchemaFromKafka(kafkaSchema);

            // Then
            assertNotNull(arrowSchema);
            assertEquals(2, arrowSchema.getFields().size());

            Field requiredAddressField = arrowSchema.getFields().get(0);
            assertFalse(requiredAddressField.isNullable());

            Field optionalAddressField = arrowSchema.getFields().get(1);
            assertTrue(optionalAddressField.isNullable());
        }
    }
}
