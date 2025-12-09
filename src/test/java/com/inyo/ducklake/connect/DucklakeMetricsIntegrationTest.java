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

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.inyo.ducklake.TestHelper;
import com.inyo.ducklake.ingestor.DucklakeWriter;
import com.inyo.ducklake.ingestor.DucklakeWriterConfig;
import java.io.File;
import java.util.ArrayList;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.kafka.common.metrics.Metrics;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Integration test demonstrating metrics collection during actual DuckDB operations. */
class DucklakeMetricsIntegrationTest {

  @TempDir File tempDir;

  private DuckDBConnection connection;
  private BufferAllocator allocator;
  private Metrics metricsRegistry;
  private DucklakeMetrics ducklakeMetrics;

  @BeforeEach
  void setUp() throws Exception {
    // Use shared test helper that sets up a ducklake catalog for tests
    connection = TestHelper.setupDucklakeConnection();

    allocator = new RootAllocator(Long.MAX_VALUE);
    metricsRegistry = new Metrics();
    ducklakeMetrics = new DucklakeMetrics(metricsRegistry, "test-connector", "0");
  }

  @AfterEach
  void tearDown() throws Exception {
    if (ducklakeMetrics != null) {
      ducklakeMetrics.close();
    }
    if (metricsRegistry != null) {
      metricsRegistry.close();
    }
    if (allocator != null) {
      allocator.close();
    }
    if (connection != null) {
      connection.close();
    }
  }

  @Test
  void testMetricsCollectionDuringWrite() throws Exception {
    // Create a writer with metrics
    var config = new DucklakeWriterConfig("test_table", true, new String[] {"id"}, new String[] {});

    try (var writer = new DucklakeWriter(connection, config, ducklakeMetrics)) {
      // Create test data
      var schema = createTestSchema();
      try (var root = VectorSchemaRoot.create(schema, allocator)) {
        // Write first batch
        populateVectorSchemaRoot(root, 0, 10);
        writer.write(root);

        // Write second batch (will trigger MERGE)
        root.clear();
        populateVectorSchemaRoot(root, 5, 10);
        writer.write(root);

        // Write third batch
        root.clear();
        populateVectorSchemaRoot(root, 10, 10);
        writer.write(root);
      }
    }

    // Verify metrics were recorded
    var jdbcQueryCount =
        metricsRegistry.metrics().entrySet().stream()
            .filter(entry -> entry.getKey().name().equals("jdbc-query-count"))
            .findFirst();

    assertTrue(jdbcQueryCount.isPresent(), "JDBC query count metric should exist");
    var queryCount = (Double) jdbcQueryCount.get().getValue().metricValue();
    assertTrue(
        queryCount >= 3, "Should have executed at least 3 queries (INSERT + MERGE + INSERT)");

    // Verify schema operation metrics
    var schemaOpCount =
        metricsRegistry.metrics().entrySet().stream()
            .filter(entry -> entry.getKey().name().equals("schema-operation-count"))
            .findFirst();

    assertTrue(schemaOpCount.isPresent(), "Schema operation count metric should exist");
    var schemaCount = (Double) schemaOpCount.get().getValue().metricValue();
    assertTrue(schemaCount >= 1, "Should have executed at least 1 schema operation (CREATE TABLE)");

    // Verify average query time is recorded
    var avgQueryTime =
        metricsRegistry.metrics().entrySet().stream()
            .filter(entry -> entry.getKey().name().equals("jdbc-query-time-avg"))
            .findFirst();

    assertTrue(avgQueryTime.isPresent(), "Average query time metric should exist");
    var avgTime = (Double) avgQueryTime.get().getValue().metricValue();
    assertTrue(avgTime > 0, "Average query time should be greater than 0");

    System.out.println("=== Metrics Summary ===");
    System.out.println("Total JDBC Queries: " + queryCount);
    System.out.println("Total Schema Operations: " + schemaCount);
    System.out.println("Average Query Time: " + avgTime + "ms");

    // Print all metrics for debugging
    System.out.println("\n=== All Metrics ===");
    metricsRegistry
        .metrics()
        .forEach(
            (name, metric) -> {
              if (name.group().equals("ducklake-sink-task-metrics")) {
                System.out.println(name.name() + " = " + metric.metricValue());
              }
            });
  }

  @Test
  void testMetricsForMultipleTables() throws Exception {
    // Create writers for multiple tables
    var config1 = new DucklakeWriterConfig("table1", true, new String[] {"id"}, new String[] {});
    var config2 = new DucklakeWriterConfig("table2", true, new String[] {"id"}, new String[] {});

    try (var writer1 = new DucklakeWriter(connection, config1, ducklakeMetrics);
        var writer2 = new DucklakeWriter(connection, config2, ducklakeMetrics)) {

      var schema = createTestSchema();

      // Write to first table
      try (var root = VectorSchemaRoot.create(schema, allocator)) {
        populateVectorSchemaRoot(root, 0, 5);
        writer1.write(root);
      }

      // Write to second table
      try (var root = VectorSchemaRoot.create(schema, allocator)) {
        populateVectorSchemaRoot(root, 0, 5);
        writer2.write(root);
      }
    }

    // Verify metrics captured operations from both tables
    var jdbcQueryCount =
        metricsRegistry.metrics().entrySet().stream()
            .filter(entry -> entry.getKey().name().equals("jdbc-query-count"))
            .findFirst();

    assertTrue(jdbcQueryCount.isPresent());
    var queryCount = (Double) jdbcQueryCount.get().getValue().metricValue();
    assertTrue(queryCount >= 2, "Should have executed at least 2 queries (one INSERT per table)");

    var schemaOpCount =
        metricsRegistry.metrics().entrySet().stream()
            .filter(entry -> entry.getKey().name().equals("schema-operation-count"))
            .findFirst();

    assertTrue(schemaOpCount.isPresent());
    var schemaCount = (Double) schemaOpCount.get().getValue().metricValue();
    assertTrue(
        schemaCount >= 2,
        "Should have executed at least 2 schema operations (one CREATE per table)");
  }

  private Schema createTestSchema() {
    var fields = new ArrayList<Field>();
    fields.add(new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null));
    fields.add(new Field("name", FieldType.nullable(new ArrowType.Utf8()), null));
    return new Schema(fields);
  }

  private void populateVectorSchemaRoot(VectorSchemaRoot root, int startId, int count) {
    var idVector = (BigIntVector) root.getVector("id");
    var nameVector = (VarCharVector) root.getVector("name");

    for (var i = 0; i < count; i++) {
      var id = startId + i;
      idVector.setSafe(i, id);
      nameVector.setSafe(i, ("name_" + id).getBytes());
    }

    root.setRowCount(count);
  }
}
