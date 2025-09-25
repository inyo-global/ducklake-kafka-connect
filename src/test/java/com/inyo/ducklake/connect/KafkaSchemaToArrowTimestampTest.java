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

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Time;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("KafkaSchemaToArrow Timestamp Support Tests")
class KafkaSchemaToArrowTimestampTest {

    @Nested
    @DisplayName("Logical Type Conversion Tests")
    class LogicalTypeConversionTests {

        @Test
        @DisplayName("Should convert Kafka Timestamp schema to Arrow Timestamp")
        void shouldConvertTimestampSchema() {
            // Create Kafka schema with timestamp logical type
            org.apache.kafka.connect.data.Schema kafkaSchema = SchemaBuilder.struct()
                .field("created_at", Timestamp.SCHEMA)
                .field("updated_at", SchemaBuilder.int64().name("org.apache.kafka.connect.data.Timestamp").build())
                .build();

            Schema arrowSchema = KafkaSchemaToArrow.arrowSchemaFromKafka(kafkaSchema);

            assertEquals(2, arrowSchema.getFields().size());

            Field createdAtField = arrowSchema.findField("created_at");
            Field updatedAtField = arrowSchema.findField("updated_at");

            assertNotNull(createdAtField);
            assertNotNull(updatedAtField);

            assertInstanceOf(ArrowType.Timestamp.class, createdAtField.getType());
            assertInstanceOf(ArrowType.Timestamp.class, updatedAtField.getType());

            ArrowType.Timestamp createdAtType = (ArrowType.Timestamp) createdAtField.getType();
            ArrowType.Timestamp updatedAtType = (ArrowType.Timestamp) updatedAtField.getType();

            assertEquals(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, createdAtType.getUnit());
            assertEquals(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, updatedAtType.getUnit());
        }

        @Test
        @DisplayName("Should convert Kafka Date schema to Arrow Date")
        void shouldConvertDateSchema() {
            org.apache.kafka.connect.data.Schema kafkaSchema = SchemaBuilder.struct()
                .field("birth_date", Date.SCHEMA)
                .field("hire_date", SchemaBuilder.int32().name("org.apache.kafka.connect.data.Date").build())
                .build();

            Schema arrowSchema = KafkaSchemaToArrow.arrowSchemaFromKafka(kafkaSchema);

            Field birthDateField = arrowSchema.findField("birth_date");
            Field hireDateField = arrowSchema.findField("hire_date");

            assertNotNull(birthDateField);
            assertNotNull(hireDateField);

            assertInstanceOf(ArrowType.Date.class, birthDateField.getType());
            assertInstanceOf(ArrowType.Date.class, hireDateField.getType());

            ArrowType.Date birthDateType = (ArrowType.Date) birthDateField.getType();
            ArrowType.Date hireDateType = (ArrowType.Date) hireDateField.getType();

            assertEquals(org.apache.arrow.vector.types.DateUnit.DAY, birthDateType.getUnit());
            assertEquals(org.apache.arrow.vector.types.DateUnit.DAY, hireDateType.getUnit());
        }

        @Test
        @DisplayName("Should convert Kafka Time schema to Arrow Time")
        void shouldConvertTimeSchema() {
            org.apache.kafka.connect.data.Schema kafkaSchema = SchemaBuilder.struct()
                .field("start_time", Time.SCHEMA)
                .field("end_time", SchemaBuilder.int32().name("org.apache.kafka.connect.data.Time").build())
                .build();

            Schema arrowSchema = KafkaSchemaToArrow.arrowSchemaFromKafka(kafkaSchema);

            Field startTimeField = arrowSchema.findField("start_time");
            Field endTimeField = arrowSchema.findField("end_time");

            assertNotNull(startTimeField);
            assertNotNull(endTimeField);

            assertInstanceOf(ArrowType.Time.class, startTimeField.getType());
            assertInstanceOf(ArrowType.Time.class, endTimeField.getType());

            ArrowType.Time startTimeType = (ArrowType.Time) startTimeField.getType();
            ArrowType.Time endTimeType = (ArrowType.Time) endTimeField.getType();

            assertEquals(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, startTimeType.getUnit());
            assertEquals(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, endTimeType.getUnit());
            assertEquals(32, startTimeType.getBitWidth());
            assertEquals(32, endTimeType.getBitWidth());
        }

        @Test
        @DisplayName("Should handle nullable timestamp fields")
        void shouldHandleNullableTimestampFields() {
            org.apache.kafka.connect.data.Schema kafkaSchema = SchemaBuilder.struct()
                .field("optional_timestamp", SchemaBuilder.int64()
                    .name("org.apache.kafka.connect.data.Timestamp")
                    .optional()
                    .build())
                .field("required_timestamp", SchemaBuilder.int64()
                    .name("org.apache.kafka.connect.data.Timestamp")
                    .build())
                .build();

            Schema arrowSchema = KafkaSchemaToArrow.arrowSchemaFromKafka(kafkaSchema);

            Field optionalField = arrowSchema.findField("optional_timestamp");
            Field requiredField = arrowSchema.findField("required_timestamp");

            assertNotNull(optionalField);
            assertNotNull(requiredField);

            assertTrue(optionalField.isNullable());
            assertFalse(requiredField.isNullable());

            assertInstanceOf(ArrowType.Timestamp.class, optionalField.getType());
            assertInstanceOf(ArrowType.Timestamp.class, requiredField.getType());
        }
    }

    @Nested
    @DisplayName("Complex Schema Tests")
    class ComplexSchemaTests {

        @Test
        @DisplayName("Should handle nested structures with timestamps")
        void shouldHandleNestedStructuresWithTimestamps() {
            org.apache.kafka.connect.data.Schema addressSchema = SchemaBuilder.struct()
                .field("street", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                .field("created_at", Timestamp.SCHEMA)
                .build();

            org.apache.kafka.connect.data.Schema kafkaSchema = SchemaBuilder.struct()
                .field("id", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
                .field("name", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                .field("created_at", Timestamp.SCHEMA)
                .field("address", addressSchema)
                .build();

            Schema arrowSchema = KafkaSchemaToArrow.arrowSchemaFromKafka(kafkaSchema);

            Field rootTimestampField = arrowSchema.findField("created_at");
            Field addressField = arrowSchema.findField("address");

            assertNotNull(rootTimestampField);
            assertNotNull(addressField);

            assertInstanceOf(ArrowType.Timestamp.class, rootTimestampField.getType());
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

        @Test
        @DisplayName("Should handle arrays with timestamp elements")
        void shouldHandleArraysWithTimestampElements() {
            org.apache.kafka.connect.data.Schema kafkaSchema = SchemaBuilder.struct()
                .field("timestamps", SchemaBuilder.array(Timestamp.SCHEMA).build())
                .build();

            Schema arrowSchema = KafkaSchemaToArrow.arrowSchemaFromKafka(kafkaSchema);

            Field timestampsField = arrowSchema.findField("timestamps");
            assertNotNull(timestampsField);

            assertInstanceOf(ArrowType.List.class, timestampsField.getType());

            // Check array element type
            assertEquals(1, timestampsField.getChildren().size());
            Field elementField = timestampsField.getChildren().get(0);
            assertInstanceOf(ArrowType.Timestamp.class, elementField.getType());
        }

        @Test
        @DisplayName("Should handle maps with timestamp values")
        void shouldHandleMapsWithTimestampValues() {
            org.apache.kafka.connect.data.Schema kafkaSchema = SchemaBuilder.struct()
                .field("event_times", SchemaBuilder.map(
                    org.apache.kafka.connect.data.Schema.STRING_SCHEMA,
                    Timestamp.SCHEMA
                ).build())
                .build();

            Schema arrowSchema = KafkaSchemaToArrow.arrowSchemaFromKafka(kafkaSchema);

            Field eventTimesField = arrowSchema.findField("event_times");
            assertNotNull(eventTimesField);

            assertInstanceOf(ArrowType.Map.class, eventTimesField.getType());

            // Check map structure
            assertEquals(1, eventTimesField.getChildren().size());
            Field entriesField = eventTimesField.getChildren().get(0);
            assertEquals("entries", entriesField.getName());

            // Check key and value types
            Field keyField = null;
            Field valueField = null;
            for (Field child : entriesField.getChildren()) {
                if ("key".equals(child.getName())) {
                    keyField = child;
                } else if ("value".equals(child.getName())) {
                    valueField = child;
                }
            }

            assertNotNull(keyField);
            assertNotNull(valueField);
            assertInstanceOf(ArrowType.Utf8.class, keyField.getType());
            assertInstanceOf(ArrowType.Timestamp.class, valueField.getType());
        }
    }

    @Nested
    @DisplayName("Backward Compatibility Tests")
    class BackwardCompatibilityTests {

        @Test
        @DisplayName("Should preserve existing non-logical type conversions")
        void shouldPreserveExistingConversions() {
            org.apache.kafka.connect.data.Schema kafkaSchema = SchemaBuilder.struct()
                .field("id", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
                .field("name", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                .field("active", org.apache.kafka.connect.data.Schema.BOOLEAN_SCHEMA)
                .field("score", org.apache.kafka.connect.data.Schema.FLOAT64_SCHEMA)
                .build();

            Schema arrowSchema = KafkaSchemaToArrow.arrowSchemaFromKafka(kafkaSchema);

            Field idField = arrowSchema.findField("id");
            Field nameField = arrowSchema.findField("name");
            Field activeField = arrowSchema.findField("active");
            Field scoreField = arrowSchema.findField("score");

            assertInstanceOf(ArrowType.Int.class, idField.getType());
            assertInstanceOf(ArrowType.Utf8.class, nameField.getType());
            assertInstanceOf(ArrowType.Bool.class, activeField.getType());
            assertInstanceOf(ArrowType.FloatingPoint.class, scoreField.getType());
        }

        @Test
        @DisplayName("Should handle schemas without logical type names")
        void shouldHandleSchemasWithoutLogicalTypeNames() {
            // Regular INT64 schema without logical type name should map to Arrow Int
            org.apache.kafka.connect.data.Schema kafkaSchema = SchemaBuilder.struct()
                .field("regular_long", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
                .field("timestamp_long", Timestamp.SCHEMA)
                .build();

            Schema arrowSchema = KafkaSchemaToArrow.arrowSchemaFromKafka(kafkaSchema);

            Field regularLongField = arrowSchema.findField("regular_long");
            Field timestampLongField = arrowSchema.findField("timestamp_long");

            assertInstanceOf(ArrowType.Int.class, regularLongField.getType());
            assertInstanceOf(ArrowType.Timestamp.class, timestampLongField.getType());
        }
    }
}
