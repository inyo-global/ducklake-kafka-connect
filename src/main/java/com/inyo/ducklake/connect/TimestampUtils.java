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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class for timestamp detection and conversion */
public final class TimestampUtils {

  private static final Logger LOG = LoggerFactory.getLogger(TimestampUtils.class);

  private static final Pattern ISO8601_PATTERN =
      Pattern.compile(
          "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{1,9})?(?:Z|[+-]\\d{2}:?\\d{2})?$");

  private static final Pattern SIMPLE_TIMESTAMP_PATTERN =
      Pattern.compile("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{1,9})?$");

  private TimestampUtils() {
    // Utility class
  }

  /** Checks if a string represents a valid timestamp */
  public static boolean isTimestamp(String value) {
    if (value == null || value.trim().isEmpty()) {
      return false;
    }

    String trimmed = value.trim();

    if (!ISO8601_PATTERN.matcher(trimmed).matches()
        && !SIMPLE_TIMESTAMP_PATTERN.matcher(trimmed).matches()) {
      return false;
    }

    try {
      parseTimestampToEpochMillis(trimmed);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  /** Converts a timestamp string to epoch millis */
  public static long parseTimestampToEpochMillis(String value) {
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException("Timestamp value cannot be null or empty");
    }

    String trimmed = value.trim();

    try {
      if (trimmed.endsWith("Z")) {
        return Instant.parse(trimmed).toEpochMilli();
      } else if (trimmed.matches(".*[+-]\\d{2}:?\\d{2}$")) {
        // Normalize timezone format - add colon if missing
        String normalized = trimmed;
        if (trimmed.matches(".*[+-]\\d{4}$")) {
          // Format like "+0300" -> "+03:00"
          int lastIndex = trimmed.length();
          normalized = trimmed.substring(0, lastIndex - 2) + ":" + trimmed.substring(lastIndex - 2);
        }
        return OffsetDateTime.parse(normalized).toInstant().toEpochMilli();
      } else {
        if (trimmed.matches("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?$")) {
          LocalDateTime ldt = LocalDateTime.parse(trimmed);
          return ldt.atOffset(java.time.ZoneOffset.UTC).toInstant().toEpochMilli();
        } else {
          throw new DateTimeParseException("Invalid timestamp format", trimmed, 0);
        }
      }
    } catch (DateTimeParseException e) {
      LOG.warn("Failed to parse timestamp: {}, error: {}", trimmed, e.getMessage());
      throw new IllegalArgumentException("Invalid timestamp format: " + trimmed, e);
    }
  }
}
