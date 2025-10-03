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
 * Tests for the new partition expressions functionality in DucklakeSinkConfig. Verifies that
 * partition expressions are properly parsed and returned from configuration.
 */
class DucklakeSinkConfigPartitionTest {

  private DucklakeSinkConfig createConfig(Map<String, Object> properties) {
    return new DucklakeSinkConfig(DucklakeSinkConfig.CONFIG_DEF, properties);
  }

  @Test
  @DisplayName("Returns empty array when no partition expressions configured")
  void testNoPartitionExpressions() {
    Map<String, Object> props = new HashMap<>();
    props.put("ducklake.catalog_uri", "test://localhost");
    props.put("ducklake.data_path", "s3://test-bucket/");
    props.put("s3.url_style", "path");
    props.put("s3.use_ssl", "true");
    props.put("s3.endpoint", "localhost:9000");
    props.put("s3.access_key_id", "test");
    props.put("s3.secret_access_key", "test");

    DucklakeSinkConfig config = createConfig(props);
    String[] expressions = config.getTablePartitionByExpressions("users");

    assertArrayEquals(
        new String[0],
        expressions,
        "Should return empty array when no partition expressions configured");
  }

  @Test
  @DisplayName("Parses single column partition expression")
  void testSingleColumnPartition() {
    Map<String, Object> props = new HashMap<>();
    props.put("ducklake.catalog_uri", "test://localhost");
    props.put("ducklake.data_path", "s3://test-bucket/");
    props.put("s3.url_style", "path");
    props.put("s3.use_ssl", "true");
    props.put("s3.endpoint", "localhost:9000");
    props.put("s3.access_key_id", "test");
    props.put("s3.secret_access_key", "test");
    props.put("ducklake.table.users.partition-by", "status");

    DucklakeSinkConfig config = createConfig(props);
    String[] expressions = config.getTablePartitionByExpressions("users");

    assertArrayEquals(
        new String[] {"status"}, expressions, "Should parse single column partition expression");
  }

  @Test
  @DisplayName("Parses single temporal function partition expression")
  void testSingleTemporalFunctionPartition() {
    Map<String, Object> props = new HashMap<>();
    props.put("ducklake.catalog_uri", "test://localhost");
    props.put("ducklake.data_path", "s3://test-bucket/");
    props.put("s3.url_style", "path");
    props.put("s3.use_ssl", "true");
    props.put("s3.endpoint", "localhost:9000");
    props.put("s3.access_key_id", "test");
    props.put("s3.secret_access_key", "test");
    props.put("ducklake.table.events.partition-by", "year(created_at)");

    DucklakeSinkConfig config = createConfig(props);
    String[] expressions = config.getTablePartitionByExpressions("events");

    assertArrayEquals(
        new String[] {"year(created_at)"},
        expressions,
        "Should parse single temporal function expression");
  }

  @Test
  @DisplayName("Parses multiple column partition expressions")
  void testMultipleColumnPartitions() {
    Map<String, Object> props = new HashMap<>();
    props.put("ducklake.catalog_uri", "test://localhost");
    props.put("ducklake.data_path", "s3://test-bucket/");
    props.put("s3.url_style", "path");
    props.put("s3.use_ssl", "true");
    props.put("s3.endpoint", "localhost:9000");
    props.put("s3.access_key_id", "test");
    props.put("s3.secret_access_key", "test");
    props.put("ducklake.table.users.partition-by", "region,status,department_id");

    DucklakeSinkConfig config = createConfig(props);
    String[] expressions = config.getTablePartitionByExpressions("users");

    assertArrayEquals(
        new String[] {"region", "status", "department_id"},
        expressions,
        "Should parse multiple column expressions");
  }

  @Test
  @DisplayName("Parses multiple temporal function partition expressions")
  void testMultipleTemporalFunctionPartitions() {
    Map<String, Object> props = new HashMap<>();
    props.put("ducklake.catalog_uri", "test://localhost");
    props.put("ducklake.data_path", "s3://test-bucket/");
    props.put("s3.url_style", "path");
    props.put("s3.use_ssl", "true");
    props.put("s3.endpoint", "localhost:9000");
    props.put("s3.access_key_id", "test");
    props.put("s3.secret_access_key", "test");
    props.put("ducklake.table.events.partition-by", "year(created_at),month(created_at)");

    DucklakeSinkConfig config = createConfig(props);
    String[] expressions = config.getTablePartitionByExpressions("events");

    assertArrayEquals(
        new String[] {"year(created_at)", "month(created_at)"},
        expressions,
        "Should parse multiple temporal function expressions");
  }

  @Test
  @DisplayName("Parses mixed partition expressions")
  void testMixedPartitionExpressions() {
    Map<String, Object> props = new HashMap<>();
    props.put("ducklake.catalog_uri", "test://localhost");
    props.put("ducklake.data_path", "s3://test-bucket/");
    props.put("s3.url_style", "path");
    props.put("s3.use_ssl", "true");
    props.put("s3.endpoint", "localhost:9000");
    props.put("s3.access_key_id", "test");
    props.put("s3.secret_access_key", "test");
    props.put("ducklake.table.logs.partition-by", "year(timestamp),level,service_name");

    DucklakeSinkConfig config = createConfig(props);
    String[] expressions = config.getTablePartitionByExpressions("logs");

    assertArrayEquals(
        new String[] {"year(timestamp)", "level", "service_name"},
        expressions,
        "Should parse mixed partition expressions");
  }

  @Test
  @DisplayName("Handles whitespace in partition expressions")
  void testWhitespaceHandling() {
    Map<String, Object> props = new HashMap<>();
    props.put("ducklake.catalog_uri", "test://localhost");
    props.put("ducklake.data_path", "s3://test-bucket/");
    props.put("s3.url_style", "path");
    props.put("s3.use_ssl", "true");
    props.put("s3.endpoint", "localhost:9000");
    props.put("s3.access_key_id", "test");
    props.put("s3.secret_access_key", "test");
    props.put("ducklake.table.users.partition-by", " region , status , year(created_at) ");

    DucklakeSinkConfig config = createConfig(props);
    String[] expressions = config.getTablePartitionByExpressions("users");

    assertArrayEquals(
        new String[] {"region", "status", "year(created_at)"},
        expressions,
        "Should handle whitespace in expressions");
  }

  @Test
  @DisplayName("Handles empty partition expression configuration")
  void testEmptyPartitionExpression() {
    Map<String, Object> props = new HashMap<>();
    props.put("ducklake.catalog_uri", "test://localhost");
    props.put("ducklake.data_path", "s3://test-bucket/");
    props.put("s3.url_style", "path");
    props.put("s3.use_ssl", "true");
    props.put("s3.endpoint", "localhost:9000");
    props.put("s3.access_key_id", "test");
    props.put("s3.secret_access_key", "test");
    props.put("ducklake.table.users.partition-by", "");

    DucklakeSinkConfig config = createConfig(props);
    String[] expressions = config.getTablePartitionByExpressions("users");

    assertArrayEquals(
        new String[0], expressions, "Should return empty array for empty partition expression");
  }

  @Test
  @DisplayName("Handles whitespace-only partition expression configuration")
  void testWhitespaceOnlyPartitionExpression() {
    Map<String, Object> props = new HashMap<>();
    props.put("ducklake.catalog_uri", "test://localhost");
    props.put("ducklake.data_path", "s3://test-bucket/");
    props.put("s3.url_style", "path");
    props.put("s3.use_ssl", "true");
    props.put("s3.endpoint", "localhost:9000");
    props.put("s3.access_key_id", "test");
    props.put("s3.secret_access_key", "test");
    props.put("ducklake.table.users.partition-by", "   ");

    DucklakeSinkConfig config = createConfig(props);
    String[] expressions = config.getTablePartitionByExpressions("users");

    assertArrayEquals(
        new String[0],
        expressions,
        "Should return empty array for whitespace-only partition expression");
  }

  @Test
  @DisplayName("Parses complex temporal expressions")
  void testComplexTemporalExpressions() {
    Map<String, Object> props = new HashMap<>();
    props.put("ducklake.catalog_uri", "test://localhost");
    props.put("ducklake.data_path", "s3://test-bucket/");
    props.put("s3.url_style", "path");
    props.put("s3.use_ssl", "true");
    props.put("s3.endpoint", "localhost:9000");
    props.put("s3.access_key_id", "test");
    props.put("s3.secret_access_key", "test");
    props.put(
        "ducklake.table.events.partition-by", "year(created_at),month(created_at),day(created_at)");

    DucklakeSinkConfig config = createConfig(props);
    String[] expressions = config.getTablePartitionByExpressions("events");

    assertArrayEquals(
        new String[] {"year(created_at)", "month(created_at)", "day(created_at)"},
        expressions,
        "Should parse complex temporal expressions");
  }

  @Test
  @DisplayName("Different tables can have different partition expressions")
  void testDifferentTablesPartitionExpressions() {
    Map<String, Object> props = getStringObjectMap();

    DucklakeSinkConfig config = createConfig(props);

    assertArrayEquals(
        new String[] {"region", "status"}, config.getTablePartitionByExpressions("users"));
    assertArrayEquals(
        new String[] {"year(created_at)", "month(created_at)"},
        config.getTablePartitionByExpressions("events"));
    assertArrayEquals(new String[] {"level"}, config.getTablePartitionByExpressions("logs"));
    assertArrayEquals(
        new String[0], config.getTablePartitionByExpressions("orders")); // Not configured
  }

  @NotNull
  private static Map<String, Object> getStringObjectMap() {
    Map<String, Object> props = new HashMap<>();
    props.put("ducklake.catalog_uri", "test://localhost");
    props.put("ducklake.data_path", "s3://test-bucket/");
    props.put("s3.url_style", "path");
    props.put("s3.use_ssl", "true");
    props.put("s3.endpoint", "localhost:9000");
    props.put("s3.access_key_id", "test");
    props.put("s3.secret_access_key", "test");
    props.put("ducklake.table.users.partition-by", "region,status");
    props.put("ducklake.table.events.partition-by", "year(created_at),month(created_at)");
    props.put("ducklake.table.logs.partition-by", "level");
    return props;
  }
}
