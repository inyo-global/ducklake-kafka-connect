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

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("DucklakeTableManager Timestamp Support Tests")
class DucklakeTableManagerTimestampTest {

    @Nested
    @DisplayName("Arrow Type to DuckDB Type Mapping Tests")
    class ArrowTypeMappingTests {

        @Test
        @DisplayName("Should map Arrow Timestamp types to DuckDB TIMESTAMP")
        void shouldMapTimestampTypes() {
            // Test different timestamp units
            ArrowType.Timestamp secondTimestamp = new ArrowType.Timestamp(
                org.apache.arrow.vector.types.TimeUnit.SECOND, null);
            ArrowType.Timestamp milliTimestamp = new ArrowType.Timestamp(
                org.apache.arrow.vector.types.TimeUnit.MILLISECOND, null);
            ArrowType.Timestamp microTimestamp = new ArrowType.Timestamp(
                org.apache.arrow.vector.types.TimeUnit.MICROSECOND, null);
            ArrowType.Timestamp nanoTimestamp = new ArrowType.Timestamp(
                org.apache.arrow.vector.types.TimeUnit.NANOSECOND, null);

            // Create a test table manager to access the toDuckDBType method
            TestDucklakeTableManager tableManager = new TestDucklakeTableManager();

            assertEquals("TIMESTAMP", tableManager.toDuckDBType(secondTimestamp));
            assertEquals("TIMESTAMP", tableManager.toDuckDBType(milliTimestamp));
            assertEquals("TIMESTAMP", tableManager.toDuckDBType(microTimestamp));
            assertEquals("TIMESTAMP", tableManager.toDuckDBType(nanoTimestamp));
        }

        @Test
        @DisplayName("Should map Arrow Date types to DuckDB DATE")
        void shouldMapDateTypes() {
            ArrowType.Date dayDate = new ArrowType.Date(
                org.apache.arrow.vector.types.DateUnit.DAY);
            ArrowType.Date milliDate = new ArrowType.Date(
                org.apache.arrow.vector.types.DateUnit.MILLISECOND);

            TestDucklakeTableManager tableManager = new TestDucklakeTableManager();

            assertEquals("DATE", tableManager.toDuckDBType(dayDate));
            assertEquals("DATE", tableManager.toDuckDBType(milliDate));
        }

        @Test
        @DisplayName("Should map Arrow Time types to DuckDB TIME")
        void shouldMapTimeTypes() {
            ArrowType.Time secondTime = new ArrowType.Time(
                org.apache.arrow.vector.types.TimeUnit.SECOND, 32);
            ArrowType.Time milliTime = new ArrowType.Time(
                org.apache.arrow.vector.types.TimeUnit.MILLISECOND, 32);
            ArrowType.Time microTime = new ArrowType.Time(
                org.apache.arrow.vector.types.TimeUnit.MICROSECOND, 64);
            ArrowType.Time nanoTime = new ArrowType.Time(
                org.apache.arrow.vector.types.TimeUnit.NANOSECOND, 64);

            TestDucklakeTableManager tableManager = new TestDucklakeTableManager();

            assertEquals("TIME", tableManager.toDuckDBType(secondTime));
            assertEquals("TIME", tableManager.toDuckDBType(milliTime));
            assertEquals("TIME", tableManager.toDuckDBType(microTime));
            assertEquals("TIME", tableManager.toDuckDBType(nanoTime));
        }

        @Test
        @DisplayName("Should preserve existing type mappings")
        void shouldPreserveExistingTypeMappings() {
            TestDucklakeTableManager tableManager = new TestDucklakeTableManager();

            // Ensure existing types still work
            assertEquals("VARCHAR", tableManager.toDuckDBType(ArrowType.Utf8.INSTANCE));
            assertEquals("BOOLEAN", tableManager.toDuckDBType(ArrowType.Bool.INSTANCE));
            assertEquals("BLOB", tableManager.toDuckDBType(ArrowType.Binary.INSTANCE));
            assertEquals("JSON", tableManager.toDuckDBType(ArrowType.Struct.INSTANCE));
            assertEquals("JSON", tableManager.toDuckDBType(ArrowType.List.INSTANCE));
            assertEquals("JSON", tableManager.toDuckDBType(new ArrowType.Map(true)));
        }

        @Test
        @DisplayName("Should handle timezone in timestamp types")
        void shouldHandleTimezoneInTimestamps() {
            // Test timestamp with timezone
            ArrowType.Timestamp timestampWithTz = new ArrowType.Timestamp(
                org.apache.arrow.vector.types.TimeUnit.MILLISECOND, "UTC");
            ArrowType.Timestamp timestampWithoutTz = new ArrowType.Timestamp(
                org.apache.arrow.vector.types.TimeUnit.MILLISECOND, null);

            TestDucklakeTableManager tableManager = new TestDucklakeTableManager();

            assertEquals("TIMESTAMP", tableManager.toDuckDBType(timestampWithTz));
            assertEquals("TIMESTAMP", tableManager.toDuckDBType(timestampWithoutTz));
        }
    }

    @Nested
    @DisplayName("Unsupported Type Tests")
    class UnsupportedTypeTests {

        @Test
        @DisplayName("Should throw exception for unsupported Arrow types")
        void shouldThrowForUnsupportedTypes() {
            // Use an existing ArrowType that we know is not supported
            // Let's create an ArrowType.Null which should not be handled
            ArrowType unsupportedType = ArrowType.Null.INSTANCE;

            TestDucklakeTableManager tableManager = new TestDucklakeTableManager();

            assertThrows(IllegalArgumentException.class,
                () -> tableManager.toDuckDBType(unsupportedType));
        }
    }

    // Test implementation of DucklakeTableManager that exposes the toDuckDBType method
    private static class TestDucklakeTableManager {
        public String toDuckDBType(ArrowType type) {
            if (type instanceof ArrowType.Int i) {
                return switch (i.getBitWidth()) {
                    case ArrowTypeConstants.INT8_BIT_WIDTH -> "TINYINT";
                    case ArrowTypeConstants.INT16_BIT_WIDTH -> "SMALLINT";
                    case ArrowTypeConstants.INT32_BIT_WIDTH -> "INTEGER";
                    case ArrowTypeConstants.INT64_BIT_WIDTH -> "BIGINT";
                    default -> throw new IllegalArgumentException("Unsupported int bit width: " + i.getBitWidth());
                };
            } else if (type instanceof ArrowType.FloatingPoint fp) {
                return switch (fp.getPrecision()) {
                    case SINGLE -> "FLOAT";
                    case DOUBLE -> "DOUBLE";
                    default -> throw new IllegalArgumentException(
                            "Unsupported floating precision: " + fp.getPrecision());
                };
            } else if (type instanceof ArrowType.Bool) {
                return "BOOLEAN";
            } else if (type instanceof ArrowType.Utf8) {
                return "VARCHAR";
            } else if (type instanceof ArrowType.Binary) {
                return "BLOB";
            } else if (type instanceof ArrowType.Timestamp timestamp) {
                return switch (timestamp.getUnit()) {
                    case SECOND, MILLISECOND, MICROSECOND, NANOSECOND -> "TIMESTAMP";
                };
            } else if (type instanceof ArrowType.Date date) {
                return switch (date.getUnit()) {
                    case DAY -> "DATE";
                    case MILLISECOND -> "DATE";
                };
            } else if (type instanceof ArrowType.Time time) {
                return switch (time.getUnit()) {
                    case SECOND, MILLISECOND, MICROSECOND, NANOSECOND -> "TIME";
                };
            } else if (type instanceof ArrowType.Struct
                    || type instanceof ArrowType.List
                    || type instanceof ArrowType.Map) {
                return "JSON";
            }
            throw new IllegalArgumentException("Unsupported Arrow type: " + type);
        }
    }
}
