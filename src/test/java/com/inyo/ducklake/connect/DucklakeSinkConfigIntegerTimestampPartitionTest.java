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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.util.HashMap;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for integer timestamp partition expressions in DucklakeSinkConfig. Verifies that complex
 * casting expressions for integer timestamps are properly parsed.
 */
class DucklakeSinkConfigIntegerTimestampPartitionTest {

  private DucklakeSinkConfig createConfig(Map<String, Object> properties) {
    return new DucklakeSinkConfig(DucklakeSinkConfig.CONFIG_DEF, properties);
  }

  @Test
  @DisplayName("Parses integer timestamp partition expressions with CAST")
  void testIntegerTimestampPartitionWithCast() {
    var props =
        getStringObjectMap(
            "ducklake.table.events.partition-by", "year(CAST(epoch_seconds AS TIMESTAMP))");

    DucklakeSinkConfig config = createConfig(props);
    String[] expressions = config.getTablePartitionByExpressions("events");

    assertArrayEquals(
        new String[] {"year(CAST(epoch_seconds AS TIMESTAMP))"},
        expressions,
        "Should parse CAST expression correctly");
  }

  @Test
  @DisplayName("Parses multiple integer timestamp temporal functions")
  void testMultipleIntegerTimestampTemporalFunctions() {
    var props =
        getStringObjectMap(
            "ducklake.table.user_events.partition-by",
            "year(CAST(created_epoch AS TIMESTAMP)),"
                + "month(CAST(created_epoch AS TIMESTAMP)),"
                + "day(CAST(created_epoch AS TIMESTAMP))");

    DucklakeSinkConfig config = createConfig(props);
    String[] expressions = config.getTablePartitionByExpressions("user_events");

    assertArrayEquals(
        new String[] {
          "year(CAST(created_epoch AS TIMESTAMP))",
          "month(CAST(created_epoch AS TIMESTAMP))",
          "day(CAST(created_epoch AS TIMESTAMP))"
        },
        expressions,
        "Should parse multiple CAST temporal functions");
  }

  @NotNull
  private static Map<String, Object> getStringObjectMap(String key, String value) {
    Map<String, Object> props = new HashMap<>();
    props.put("ducklake.catalog_uri", "test://localhost");
    props.put("ducklake.data_path", "s3://test-bucket/");
    props.put("s3.url_style", "path");
    props.put("s3.use_ssl", "true");
    props.put("s3.endpoint", "localhost:9000");
    props.put("s3.access_key_id", "test");
    props.put("s3.secret_access_key", "test");
    props.put("topic2table.map", "");
    props.put(key, value);
    return props;
  }

  @Test
  @DisplayName("Parses mixed integer timestamp and regular column partitioning")
  void testMixedIntegerTimestampPartitioning() {
    var props =
        getStringObjectMap(
            "ducklake.table.activity_logs.partition-by",
            "year(CAST(event_time AS TIMESTAMP)),event_type,user_segment");

    DucklakeSinkConfig config = createConfig(props);
    String[] expressions = config.getTablePartitionByExpressions("activity_logs");

    assertArrayEquals(
        new String[] {"year(CAST(event_time AS TIMESTAMP))", "event_type", "user_segment"},
        expressions,
        "Should parse mixed CAST and regular column expressions");
  }

  @Test
  @DisplayName("Parses millisecond epoch timestamp expressions")
  void testMillisecondEpochTimestampExpressions() {
    var expression =
        "year(CAST(event_time_millis / 1000 AS TIMESTAMP)),"
            + "month(CAST(event_time_millis / 1000 AS TIMESTAMP))";
    var props = getStringObjectMap("ducklake.table.metrics.partition-by", expression);

    DucklakeSinkConfig config = createConfig(props);
    String[] expressions = config.getTablePartitionByExpressions("metrics");

    assertArrayEquals(
        new String[] {
          "year(CAST(event_time_millis / 1000 AS TIMESTAMP))",
          "month(CAST(event_time_millis / 1000 AS TIMESTAMP))"
        },
        expressions,
        "Should parse millisecond conversion expressions");
  }

  @Test
  @DisplayName("Parses to_timestamp function expressions")
  void testToTimestampFunctionExpressions() {
    var props =
        getStringObjectMap(
            "ducklake.table.user_activity.partition-by",
            "year(to_timestamp(unix_time)),month(to_timestamp(unix_time))");

    DucklakeSinkConfig config = createConfig(props);
    String[] expressions = config.getTablePartitionByExpressions("user_activity");

    assertArrayEquals(
        new String[] {"year(to_timestamp(unix_time))", "month(to_timestamp(unix_time))"},
        expressions,
        "Should parse to_timestamp function expressions");
  }

  @Test
  @DisplayName("Parses complex conditional casting expressions")
  void testComplexConditionalCastingExpressions() {
    var props =
        getStringObjectMap(
            "ducklake.table.complex_events.partition-by",
            "year(CASE WHEN timestamp_type = 'seconds' THEN CAST(event_time AS TIMESTAMP) "
                + "ELSE CAST(event_time / 1000 AS TIMESTAMP) END),status");

    DucklakeSinkConfig config = createConfig(props);
    String[] expressions = config.getTablePartitionByExpressions("complex_events");

    var expected =
        new String[] {
          "year(CASE WHEN timestamp_type = 'seconds' THEN CAST(event_time AS TIMESTAMP) "
              + "ELSE CAST(event_time / 1000 AS TIMESTAMP) END)",
          "status"
        };
    assertArrayEquals(expected, expressions, "Should parse complex conditional CASE expressions");
  }

  @Test
  @DisplayName("Parses epoch timestamp with offset expressions")
  void testEpochTimestampWithOffsetExpressions() {
    var props =
        getStringObjectMap(
            "ducklake.table.legacy_data.partition-by",
            "year(CAST(epoch_time + 2208988800 AS TIMESTAMP)),region");

    DucklakeSinkConfig config = createConfig(props);
    String[] expressions = config.getTablePartitionByExpressions("legacy_data");

    assertArrayEquals(
        new String[] {"year(CAST(epoch_time + 2208988800 AS TIMESTAMP))", "region"},
        expressions,
        "Should parse epoch offset calculation expressions");
  }

  @Test
  @DisplayName("Handles whitespace in complex integer timestamp expressions")
  void testWhitespaceInComplexExpressions() {
    var props =
        getStringObjectMap(
            "ducklake.table.formatted_events.partition-by",
            " year(CAST(created_epoch AS TIMESTAMP)) , "
                + " month(CAST(created_epoch AS TIMESTAMP)) , "
                + " event_type ");

    DucklakeSinkConfig config = createConfig(props);
    String[] expressions = config.getTablePartitionByExpressions("formatted_events");

    assertArrayEquals(
        new String[] {
          "year(CAST(created_epoch AS TIMESTAMP))",
          "month(CAST(created_epoch AS TIMESTAMP))",
          "event_type"
        },
        expressions,
        "Should handle whitespace in complex expressions");
  }

  @Test
  @DisplayName("Different tables can have different integer timestamp partition expressions")
  void testDifferentTablesWithIntegerTimestampExpressions() {
    var props =
        getStringObjectMap(
            "ducklake.table.events.partition-by", "year(CAST(epoch_seconds AS TIMESTAMP))");
    props.put(
        "ducklake.table.metrics.partition-by",
        "year(to_timestamp(unix_time)),month(to_timestamp(unix_time))");
    props.put(
        "ducklake.table.logs.partition-by",
        "year(CAST(timestamp_millis / 1000 AS TIMESTAMP)),level");

    DucklakeSinkConfig config = createConfig(props);

    assertArrayEquals(
        new String[] {"year(CAST(epoch_seconds AS TIMESTAMP))"},
        config.getTablePartitionByExpressions("events"));
    assertArrayEquals(
        new String[] {"year(to_timestamp(unix_time))", "month(to_timestamp(unix_time))"},
        config.getTablePartitionByExpressions("metrics"));
    assertArrayEquals(
        new String[] {"year(CAST(timestamp_millis / 1000 AS TIMESTAMP))", "level"},
        config.getTablePartitionByExpressions("logs"));
  }
}
