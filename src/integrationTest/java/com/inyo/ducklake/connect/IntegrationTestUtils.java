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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.shaded.org.awaitility.Awaitility;

/** Shared helpers to reduce duplication across integration tests. */
public final class IntegrationTestUtils {
  private IntegrationTestUtils() {}

  public static Map<String, String> baseConnectorConfig(
      String topicName,
      String tableName,
      PostgreSQLContainer<?> postgres,
      MinIOContainer minio,
      boolean autoCreate,
      String idColumns) {
    // This variant requires concrete container instances (no null checks).
    var cfg = new HashMap<String, String>();
    var pgHost = postgres.getHost();
    var pgPort = postgres.getMappedPort(5432);
    cfg.put(
        DucklakeSinkConfig.DUCKLAKE_CATALOG_URI,
        String.format(
            "postgres:dbname=ducklake_catalog host=%s port=%d user=duck password=duck",
            pgHost, pgPort));

    cfg.put("topic2table.map", topicName + ":" + tableName);
    cfg.put("ducklake.data_path", "s3://test-bucket/");
    cfg.put("s3.url_style", "path");
    cfg.put("s3.use_ssl", "false");

    var minioHost = minio.getHost();
    var minioPort = minio.getMappedPort(9000);
    cfg.put("s3.endpoint", String.format("%s:%d", minioHost, minioPort));
    cfg.put("s3.access_key_id", "minio");
    cfg.put("s3.secret_access_key", "minio123");

    if (autoCreate) {
      cfg.put("ducklake.table." + tableName + ".auto-create", "true");
    }
    if (idColumns != null && !idColumns.isBlank()) {
      cfg.put("ducklake.table." + tableName + ".id-columns", idColumns);
    }

    // Set low flush thresholds for integration tests to avoid waiting for default 60s interval
    cfg.put(DucklakeSinkConfig.FLUSH_SIZE, "1");
    cfg.put(DucklakeSinkConfig.FLUSH_INTERVAL_MS, "1000");
    cfg.put(DucklakeSinkConfig.FILE_SIZE_BYTES, "1024");

    return cfg;
  }

  // Convenience overload that does not take container instances and uses TestConfig defaults
  public static Map<String, String> baseConnectorConfig(
      String topicName, String tableName, boolean autoCreate, String idColumns) {
    var cfg = new HashMap<String, String>();
    cfg.putAll(TestConfig.getBaseDucklakeConfig());
    cfg.put("topic2table.map", topicName + ":" + tableName);
    cfg.putIfAbsent("ducklake.data_path", "s3://test-bucket/");
    cfg.putIfAbsent("s3.url_style", "path");
    cfg.putIfAbsent("s3.use_ssl", "false");
    cfg.putIfAbsent("s3.endpoint", "localhost:9000");
    cfg.putIfAbsent("s3.access_key_id", "minio");
    cfg.putIfAbsent("s3.secret_access_key", "minio123");
    if (autoCreate) {
      cfg.put("ducklake.table." + tableName + ".auto-create", "true");
    }
    if (idColumns != null && !idColumns.isBlank()) {
      cfg.put("ducklake.table." + tableName + ".id-columns", idColumns);
    }

    // Set low flush thresholds for integration tests to avoid waiting for default 60s interval
    cfg.put(DucklakeSinkConfig.FLUSH_SIZE, "1");
    cfg.put(DucklakeSinkConfig.FLUSH_INTERVAL_MS, "1000");
    cfg.put(DucklakeSinkConfig.FILE_SIZE_BYTES, "1024");

    return cfg;
  }

  public static DucklakeConnectionFactory connectionFactory(
      String topicName, String tableName, PostgreSQLContainer<?> postgres, MinIOContainer minio) {
    var base = baseConnectorConfig(topicName, tableName, postgres, minio, true, null);
    var dlConfig = new DucklakeSinkConfig(DucklakeSinkConfig.CONFIG_DEF, base);
    return new DucklakeConnectionFactory(dlConfig);
  }

  public static DucklakeConnectionFactory connectionFactory(String topicName, String tableName) {
    var base = baseConnectorConfig(topicName, tableName, true, null);
    var dlConfig = new DucklakeSinkConfig(DucklakeSinkConfig.CONFIG_DEF, base);
    return new DucklakeConnectionFactory(dlConfig);
  }

  public static void awaitConnectorRunning(EmbeddedKafkaConnect ekc, String name) {
    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofSeconds(1))
        .until(
            () -> {
              var status = ekc.getConnectorStatus(name);
              if (status == null) return false;
              var state = status.connector().state();
              if ("FAILED".equals(state)) {
                System.err.println("Connector failed: " + status.connector().trace());
                return false;
              }
              var allTasksRunning =
                  status.tasks().stream().allMatch(t -> "RUNNING".equals(t.state()));
              return "RUNNING".equals(state) && allTasksRunning;
            });
  }

  public static void assertTableRow(
      DucklakeConnectionFactory factory,
      String tableName,
      String selectSql,
      Map<String, Object> expected) {
    try {
      factory.create();
      try (var conn = factory.getConnection();
          var st = conn.createStatement();
          var rs = st.executeQuery(selectSql)) {
        assertTrue(rs.next(), "Expected at least one row in table " + tableName);
        for (var entry : expected.entrySet()) {
          var k = entry.getKey();
          var v = entry.getValue();
          Object actual = rs.getObject(k);
          assertNotNull(actual, "Column " + k + " is null");
          assertEquals(v, actual, "Mismatch for column " + k);
        }
      }
    } catch (SQLException e) {
      if (e.getMessage() != null && e.getMessage().contains("does not exist")) {
        throw new AssertionError("Table not ready yet: " + e.getMessage(), e);
      }
      throw new RuntimeException("Failed asserting table data: " + e.getMessage(), e);
    } finally {
      factory.close();
    }
  }
}
