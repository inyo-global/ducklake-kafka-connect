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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DucklakeMetricsTest {

  private Metrics metricsRegistry;
  private DucklakeMetrics ducklakeMetrics;

  @BeforeEach
  void setUp() {
    metricsRegistry = new Metrics();
    ducklakeMetrics = new DucklakeMetrics(metricsRegistry, "test-connector", "0");
  }

  @AfterEach
  void tearDown() {
    if (ducklakeMetrics != null) {
      ducklakeMetrics.close();
    }
    if (metricsRegistry != null) {
      metricsRegistry.close();
    }
  }

  @Test
  void testMetricsAreRegistered() {
    // Verify that metrics are registered in the registry
    var metricNames = metricsRegistry.metrics().keySet();

    var hasJdbcQueryMetric =
        metricNames.stream().anyMatch(name -> name.name().equals("jdbc-query-time-avg"));

    var hasSchemaOperationMetric =
        metricNames.stream().anyMatch(name -> name.name().equals("schema-operation-time-avg"));

    var hasRecordProcessingMetric =
        metricNames.stream().anyMatch(name -> name.name().equals("records-processed-total"));

    assertTrue(hasJdbcQueryMetric, "JDBC query metric should be registered");
    assertTrue(hasSchemaOperationMetric, "Schema operation metric should be registered");
    assertTrue(hasRecordProcessingMetric, "Record processing metric should be registered");
  }

  @Test
  void testRecordJdbcQuery() {
    // Record a query execution time
    var durationNanos = 1_000_000L; // 1ms
    ducklakeMetrics.recordJdbcQuery(durationNanos);

    // Find the count metric
    var countMetric =
        metricsRegistry.metrics().entrySet().stream()
            .filter(entry -> entry.getKey().name().equals("jdbc-query-count"))
            .findFirst();

    assertTrue(countMetric.isPresent(), "Query count metric should exist");
    assertEquals(1.0, countMetric.get().getValue().metricValue(), "Query count should be 1");
  }

  @Test
  void testRecordSchemaOperation() {
    // Record a schema operation
    var durationNanos = 5_000_000L; // 5ms
    ducklakeMetrics.recordSchemaOperation(durationNanos);

    // Find the count metric
    var countMetric =
        metricsRegistry.metrics().entrySet().stream()
            .filter(entry -> entry.getKey().name().equals("schema-operation-count"))
            .findFirst();

    assertTrue(countMetric.isPresent(), "Schema operation count metric should exist");
    assertEquals(
        1.0, countMetric.get().getValue().metricValue(), "Schema operation count should be 1");
  }

  @Test
  void testRecordBatchProcessed() {
    // Record a batch of records
    var recordCount = 100;
    ducklakeMetrics.recordBatchProcessed(recordCount);

    // Find the total records metric
    var totalMetric =
        metricsRegistry.metrics().entrySet().stream()
            .filter(entry -> entry.getKey().name().equals("records-processed-total"))
            .findFirst();

    assertTrue(totalMetric.isPresent(), "Records processed total metric should exist");
    assertEquals(100.0, totalMetric.get().getValue().metricValue(), "Total records should be 100");
  }

  @Test
  void testJdbcQueryTimer() throws InterruptedException {
    // Use the timer utility
    try (var timer = ducklakeMetrics.startJdbcQueryTimer()) {
      // Simulate some work
      Thread.sleep(10);
    }

    // Find the count metric
    var countMetric =
        metricsRegistry.metrics().entrySet().stream()
            .filter(entry -> entry.getKey().name().equals("jdbc-query-count"))
            .findFirst();

    assertTrue(countMetric.isPresent(), "Query count metric should exist");
    assertEquals(1.0, countMetric.get().getValue().metricValue(), "Query count should be 1");

    // Find the avg time metric
    var avgMetric =
        metricsRegistry.metrics().entrySet().stream()
            .filter(entry -> entry.getKey().name().equals("jdbc-query-time-avg"))
            .findFirst();

    assertTrue(avgMetric.isPresent(), "Query avg time metric should exist");
    var avgTime = (Double) avgMetric.get().getValue().metricValue();
    assertTrue(avgTime > 0, "Average query time should be greater than 0");
  }

  @Test
  void testMetricTags() {
    // Verify that metrics have the correct tags
    var metricNames = metricsRegistry.metrics().keySet();

    for (MetricName metricName : metricNames) {
      if (metricName.group().equals("ducklake-sink-task-metrics")) {
        assertNotNull(metricName.tags(), "Metric should have tags");
        assertEquals("test-connector", metricName.tags().get("connector"));
        assertEquals("0", metricName.tags().get("task"));
      }
    }
  }

  @Test
  void testMultipleOperations() {
    // Record multiple operations
    ducklakeMetrics.recordJdbcQuery(1_000_000L);
    ducklakeMetrics.recordJdbcQuery(2_000_000L);
    ducklakeMetrics.recordJdbcQuery(3_000_000L);

    // Find the count metric
    var countMetric =
        metricsRegistry.metrics().entrySet().stream()
            .filter(entry -> entry.getKey().name().equals("jdbc-query-count"))
            .findFirst();

    assertTrue(countMetric.isPresent());
    assertEquals(3.0, countMetric.get().getValue().metricValue(), "Query count should be 3");

    // Find the avg time metric
    var avgMetric =
        metricsRegistry.metrics().entrySet().stream()
            .filter(entry -> entry.getKey().name().equals("jdbc-query-time-avg"))
            .findFirst();

    assertTrue(avgMetric.isPresent());
    var avgTime = (Double) avgMetric.get().getValue().metricValue();
    assertEquals(2.0, avgTime, 0.1, "Average should be 2ms");
  }

  @Test
  void testMetricsCleanup() {
    // Get initial metric count
    var initialCount = metricsRegistry.metrics().size();
    assertTrue(initialCount > 0, "Should have metrics registered");

    // Close metrics
    ducklakeMetrics.close();
    ducklakeMetrics = null;

    // Verify metrics are removed
    var finalCount = metricsRegistry.metrics().size();
    assertTrue(finalCount < initialCount, "Metrics should be removed after close");
  }
}
