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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

@DisplayName("TimestampUtils Tests")
class TimestampUtilsTest {

    @Nested
    @DisplayName("Timestamp Detection Tests")
    class TimestampDetectionTests {

        @Test
        @DisplayName("Should detect ISO 8601 timestamp with timezone")
        void shouldDetectISO8601WithTimezone() {
            assertTrue(TimestampUtils.isTimestamp("2025-09-25T18:05:12Z"));
            assertTrue(TimestampUtils.isTimestamp("2025-09-25T18:05:12+03:00"));
            assertTrue(TimestampUtils.isTimestamp("2025-09-25T18:05:12-05:00"));
            assertTrue(TimestampUtils.isTimestamp("2025-09-25T18:05:12+0300"));
        }

        @Test
        @DisplayName("Should detect ISO 8601 timestamp without timezone")
        void shouldDetectISO8601WithoutTimezone() {
            assertTrue(TimestampUtils.isTimestamp("2025-09-25T18:05:12"));
            assertTrue(TimestampUtils.isTimestamp("2025-01-01T00:00:00"));
            assertTrue(TimestampUtils.isTimestamp("2025-12-31T23:59:59"));
        }

        @Test
        @DisplayName("Should detect timestamp with milliseconds")
        void shouldDetectTimestampWithMilliseconds() {
            assertTrue(TimestampUtils.isTimestamp("2025-09-25T18:05:12.123"));
            assertTrue(TimestampUtils.isTimestamp("2025-09-25T18:05:12.123456"));
            assertTrue(TimestampUtils.isTimestamp("2025-09-25T18:05:12.123456789"));
            assertTrue(TimestampUtils.isTimestamp("2025-09-25T18:05:12.123Z"));
            assertTrue(TimestampUtils.isTimestamp("2025-09-25T18:05:12.123+03:00"));
        }

        @Test
        @DisplayName("Should handle null and empty values")
        void shouldHandleNullAndEmpty() {
            assertFalse(TimestampUtils.isTimestamp(null));
            assertFalse(TimestampUtils.isTimestamp(""));
            assertFalse(TimestampUtils.isTimestamp("   "));
        }

        @Test
        @DisplayName("Should reject invalid timestamp formats")
        void shouldRejectInvalidFormats() {
            assertFalse(TimestampUtils.isTimestamp("not-a-timestamp"));
            assertFalse(TimestampUtils.isTimestamp("2025/09/25 18:05:12"));
            assertFalse(TimestampUtils.isTimestamp("25-09-2025T18:05:12"));
            assertFalse(TimestampUtils.isTimestamp("2025-9-25T18:05:12"));
            assertFalse(TimestampUtils.isTimestamp("2025-09-25 18:05:12"));
            assertFalse(TimestampUtils.isTimestamp("2025-13-25T18:05:12"));
            assertFalse(TimestampUtils.isTimestamp("2025-09-32T18:05:12"));
        }

        @Test
        @DisplayName("Should handle whitespace correctly")
        void shouldHandleWhitespace() {
            assertTrue(TimestampUtils.isTimestamp("  2025-09-25T18:05:12  "));
            assertTrue(TimestampUtils.isTimestamp("\t2025-09-25T18:05:12Z\n"));
        }
    }

    @Nested
    @DisplayName("Timestamp Parsing Tests")
    class TimestampParsingTests {

        @Test
        @DisplayName("Should parse ISO 8601 timestamp with Z timezone")
        void shouldParseISO8601WithZ() {
            String timestampStr = "2025-09-25T18:05:12Z";
            long result = TimestampUtils.parseTimestampToEpochMillis(timestampStr);

            Instant expected = Instant.parse(timestampStr);
            assertEquals(expected.toEpochMilli(), result);
        }

        @Test
        @DisplayName("Should parse ISO 8601 timestamp with offset timezone")
        void shouldParseISO8601WithOffset() {
            String timestampStr = "2025-09-25T18:05:12+03:00";
            long result = TimestampUtils.parseTimestampToEpochMillis(timestampStr);

            Instant expected = Instant.parse(timestampStr);
            assertEquals(expected.toEpochMilli(), result);
        }

        @Test
        @DisplayName("Should parse timestamp without timezone as UTC")
        void shouldParseWithoutTimezoneAsUTC() {
            String timestampStr = "2025-09-25T18:05:12";
            long result = TimestampUtils.parseTimestampToEpochMillis(timestampStr);

            LocalDateTime ldt = LocalDateTime.parse(timestampStr);
            Instant expected = ldt.atOffset(ZoneOffset.UTC).toInstant();
            assertEquals(expected.toEpochMilli(), result);
        }

        @Test
        @DisplayName("Should parse timestamp with milliseconds")
        void shouldParseWithMilliseconds() {
            String timestampStr = "2025-09-25T18:05:12.123Z";
            long result = TimestampUtils.parseTimestampToEpochMillis(timestampStr);

            Instant expected = Instant.parse(timestampStr);
            assertEquals(expected.toEpochMilli(), result);
        }

        @Test
        @DisplayName("Should handle null and empty input")
        void shouldHandleNullAndEmptyInput() {
            assertThrows(IllegalArgumentException.class,
                () -> TimestampUtils.parseTimestampToEpochMillis(null));
            assertThrows(IllegalArgumentException.class,
                () -> TimestampUtils.parseTimestampToEpochMillis(""));
            assertThrows(IllegalArgumentException.class,
                () -> TimestampUtils.parseTimestampToEpochMillis("   "));
        }

        @Test
        @DisplayName("Should throw exception for invalid formats")
        void shouldThrowForInvalidFormats() {
            assertThrows(IllegalArgumentException.class,
                () -> TimestampUtils.parseTimestampToEpochMillis("invalid-timestamp"));
            assertThrows(IllegalArgumentException.class,
                () -> TimestampUtils.parseTimestampToEpochMillis("2025/09/25 18:05:12"));
        }

        @Test
        @DisplayName("Should handle whitespace in input")
        void shouldHandleWhitespaceInInput() {
            String timestampStr = "  2025-09-25T18:05:12Z  ";
            long result = TimestampUtils.parseTimestampToEpochMillis(timestampStr);

            Instant expected = Instant.parse("2025-09-25T18:05:12Z");
            assertEquals(expected.toEpochMilli(), result);
        }
    }

    @Nested
    @DisplayName("Edge Case Tests")
    class EdgeCaseTests {

        @Test
        @DisplayName("Should handle leap year dates")
        void shouldHandleLeapYearDates() {
            assertTrue(TimestampUtils.isTimestamp("2024-02-29T12:00:00Z"));
            long result = TimestampUtils.parseTimestampToEpochMillis("2024-02-29T12:00:00Z");
            assertTrue(result > 0);
        }

        @Test
        @DisplayName("Should handle year boundaries")
        void shouldHandleYearBoundaries() {
            assertTrue(TimestampUtils.isTimestamp("2024-12-31T23:59:59Z"));
            assertTrue(TimestampUtils.isTimestamp("2025-01-01T00:00:00Z"));

            long endOf2024 = TimestampUtils.parseTimestampToEpochMillis("2024-12-31T23:59:59Z");
            long startOf2025 = TimestampUtils.parseTimestampToEpochMillis("2025-01-01T00:00:00Z");

            assertTrue(endOf2024 < startOf2025);
        }

        @Test
        @DisplayName("Should handle various timezone formats")
        void shouldHandleVariousTimezoneFormats() {
            // All these should represent the same moment in time
            String[] timestamps = {
                "2025-09-25T18:05:12Z",
                "2025-09-25T21:05:12+03:00",
                "2025-09-25T15:05:12-03:00",
                "2025-09-25T2105+0300"  // This should fail parsing
            };

            long baseTime = TimestampUtils.parseTimestampToEpochMillis(timestamps[0]);
            assertEquals(baseTime, TimestampUtils.parseTimestampToEpochMillis(timestamps[1]));
            assertEquals(baseTime, TimestampUtils.parseTimestampToEpochMillis(timestamps[2]));

            // Invalid format should throw exception
            assertThrows(IllegalArgumentException.class,
                () -> TimestampUtils.parseTimestampToEpochMillis(timestamps[3]));
        }
    }
}
