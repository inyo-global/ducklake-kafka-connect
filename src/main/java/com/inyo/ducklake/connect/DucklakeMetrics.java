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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;

/**
 * Manages metrics for the Ducklake connector using Kafka's metrics system. Metrics are
 * automatically exposed via JMX and can be monitored through standard Kafka Connect monitoring
 * tools.
 *
 * <p>Metric groups follow Kafka Connect naming conventions: -
 * kafka.connect:type=sink-task-metrics,connector={connector-name},task={task-id}
 */
public final class DucklakeMetrics implements DucklakeMetricsInterface {

  private static final System.Logger LOG = System.getLogger(DucklakeMetrics.class.getName());
  private static final String METRIC_GROUP = "ducklake-sink-task-metrics";

  private final Metrics metrics;
  private final Map<String, String> metricTags;
  private final Sensor jdbcQuerySensor; // timing sensor for jdbc queries
  private final Sensor jdbcQueryCountSensor; // counter sensor for number of jdbc queries
  private final Sensor schemaOperationSensor; // timing sensor for schema operations
  private final Sensor schemaOperationCountSensor; // counter sensor for schema operation counts
  private final Sensor recordProcessingSensor;
  private final Sensor batchSizeSensor;

  // Operation-specific sensors
  private final Map<String, Sensor> operationTimingSensors;
  private final Map<String, Sensor> operationCountSensors;

  /**
   * Creates a new metrics instance.
   *
   * @param metrics the Kafka metrics registry (from SinkTaskContext or manually created)
   * @param connectorName the name of the connector instance
   * @param taskId the task ID
   */
  @SuppressFBWarnings(
      value = "EI_EXPOSE_REP2",
      justification =
          "Storing externally managed Metrics registry is intentional; "
              + "this class does not mutate the registry reference and registers/removes sensors appropriately.")
  public DucklakeMetrics(Metrics metrics, String connectorName, String taskId) {
    this.metrics = metrics;
    this.metricTags = new HashMap<>();
    metricTags.put("connector", connectorName);
    metricTags.put("task", taskId);

    // Initialize sensors for different metric categories
    this.jdbcQuerySensor = createJdbcQuerySensor();
    this.jdbcQueryCountSensor = createJdbcQueryCountSensor();
    this.schemaOperationSensor = createSchemaOperationSensor();
    this.schemaOperationCountSensor = createSchemaOperationCountSensor();
    this.recordProcessingSensor = createRecordProcessingSensor();
    this.batchSizeSensor = createBatchSizeSensor();

    // Initialize operation-specific sensors
    this.operationTimingSensors = new HashMap<>();
    this.operationCountSensors = new HashMap<>();
    createOperationSensor("upsertWithMergeInto");
    createOperationSensor("simpleInsert");
    createOperationSensor("createTable");
    createOperationSensor("evolveSchema");

    LOG.log(
        System.Logger.Level.INFO,
        "Initialized Ducklake metrics for connector={0}, task={1}",
        connectorName,
        taskId);
  }

  private Sensor createJdbcQueryCountSensor() {
    var sensor = metrics.sensor("jdbc-query-count-sensor");
    var countMetricName =
        new MetricName(
            "jdbc-query-count", METRIC_GROUP, "Total number of JDBC queries executed", metricTags);
    sensor.add(countMetricName, new CumulativeSum());
    return sensor;
  }

  private Sensor createJdbcQuerySensor() {
    var sensor = metrics.sensor("jdbc-query");

    // Average query time
    var avgMetricName =
        new MetricName(
            "jdbc-query-time-avg",
            METRIC_GROUP,
            "Average JDBC query execution time in ms",
            metricTags);
    sensor.add(avgMetricName, new Avg());

    // Max query time
    var maxMetricName =
        new MetricName(
            "jdbc-query-time-max",
            METRIC_GROUP,
            "Maximum JDBC query execution time in ms",
            metricTags);
    sensor.add(maxMetricName, new Max());

    // Query rate (queries per second)
    var rateMetricName =
        new MetricName(
            "jdbc-query-rate", METRIC_GROUP, "Rate of JDBC queries per second", metricTags);
    sensor.add(rateMetricName, new Rate());

    return sensor;
  }

  private Sensor createSchemaOperationCountSensor() {
    var sensor = metrics.sensor("schema-operation-count-sensor");
    var countMetricName =
        new MetricName(
            "schema-operation-count",
            METRIC_GROUP,
            "Total number of schema operations",
            metricTags);
    sensor.add(countMetricName, new CumulativeSum());
    return sensor;
  }

  private Sensor createSchemaOperationSensor() {
    var sensor = metrics.sensor("schema-operation");

    // Average schema operation time
    var avgMetricName =
        new MetricName(
            "schema-operation-time-avg",
            METRIC_GROUP,
            "Average schema operation time in ms",
            metricTags);
    sensor.add(avgMetricName, new Avg());

    // Max schema operation time
    var maxMetricName =
        new MetricName(
            "schema-operation-time-max",
            METRIC_GROUP,
            "Maximum schema operation time in ms",
            metricTags);
    sensor.add(maxMetricName, new Max());

    return sensor;
  }

  private Sensor createRecordProcessingSensor() {
    var sensor = metrics.sensor("record-processing");

    // Total records processed
    var totalMetricName =
        new MetricName(
            "records-processed-total",
            METRIC_GROUP,
            "Total number of records processed",
            metricTags);
    sensor.add(totalMetricName, new CumulativeSum());

    // Record processing rate
    var rateMetricName =
        new MetricName(
            "records-processed-rate",
            METRIC_GROUP,
            "Rate of records processed per second",
            metricTags);
    sensor.add(rateMetricName, new Rate());

    return sensor;
  }

  private Sensor createBatchSizeSensor() {
    var sensor = metrics.sensor("batch-size");

    // Average batch size
    var avgMetricName =
        new MetricName("batch-size-avg", METRIC_GROUP, "Average batch size in records", metricTags);
    sensor.add(avgMetricName, new Avg());

    // Max batch size
    var maxMetricName =
        new MetricName("batch-size-max", METRIC_GROUP, "Maximum batch size in records", metricTags);
    sensor.add(maxMetricName, new Max());

    return sensor;
  }

  private void createOperationSensor(String operationType) {
    var timingSensorName = "operation-" + operationType + "-timing";
    var timingSensor = metrics.sensor(timingSensorName);

    var operationTags = new HashMap<>(metricTags);
    operationTags.put("operation", operationType);

    // Average time for this operation
    var avgMetricName =
        new MetricName(
            "operation-time-avg",
            METRIC_GROUP,
            "Average execution time for " + operationType + " in ms",
            operationTags);
    timingSensor.add(avgMetricName, new Avg());

    // Max time for this operation
    var maxMetricName =
        new MetricName(
            "operation-time-max",
            METRIC_GROUP,
            "Maximum execution time for " + operationType + " in ms",
            operationTags);
    timingSensor.add(maxMetricName, new Max());

    // Rate for this operation
    var rateMetricName =
        new MetricName(
            "operation-rate",
            METRIC_GROUP,
            "Rate of " + operationType + " operations per second",
            operationTags);
    timingSensor.add(rateMetricName, new Rate());

    // Create a separate count sensor for this operation
    var countSensorName = "operation-" + operationType + "-count";
    var countSensor = metrics.sensor(countSensorName);
    var countMetricName =
        new MetricName(
            "operation-count",
            METRIC_GROUP,
            "Total number of " + operationType + " operations",
            operationTags);
    countSensor.add(countMetricName, new CumulativeSum());

    operationTimingSensors.put(operationType, timingSensor);
    operationCountSensors.put(operationType, countSensor);
  }

  /**
   * Records a JDBC query execution time.
   *
   * @param durationNanos the query duration in nanoseconds
   */
  @Override
  public void recordJdbcQuery(long durationNanos) {
    var durationMs = TimeUnit.NANOSECONDS.toMillis(durationNanos);
    // Record duration for timing metrics
    jdbcQuerySensor.record(durationMs);
    // Increment count metric by 1
    jdbcQueryCountSensor.record(1);
  }

  /**
   * Records a JDBC query execution time with operation type.
   *
   * @param durationNanos the query duration in nanoseconds
   * @param operationType the type of operation (e.g., "upsertWithMergeInto", "simpleInsert")
   */
  @Override
  public void recordJdbcQuery(long durationNanos, String operationType) {
    var durationMs = TimeUnit.NANOSECONDS.toMillis(durationNanos);
    // Record duration for timing metrics
    jdbcQuerySensor.record(durationMs);
    // Increment count metric by 1
    jdbcQueryCountSensor.record(1);

    // Also record to operation-specific sensors (timing + count)
    var timingSensor = operationTimingSensors.get(operationType);
    var countSensor = operationCountSensors.get(operationType);
    if (timingSensor != null) {
      timingSensor.record(durationMs);
    }
    if (countSensor != null) {
      countSensor.record(1);
    }
    if (timingSensor == null && countSensor == null) {
      LOG.log(
          System.Logger.Level.WARNING,
          "Unknown operation type: {0}. Available types: {1}",
          operationType,
          operationTimingSensors.keySet());
    }
  }

  /**
   * Records a schema operation execution time.
   *
   * @param durationNanos the operation duration in nanoseconds
   */
  @Override
  public void recordSchemaOperation(long durationNanos) {
    var durationMs = TimeUnit.NANOSECONDS.toMillis(durationNanos);
    schemaOperationSensor.record(durationMs);
    schemaOperationCountSensor.record(1);
  }

  /**
   * Records a schema operation execution time with operation type.
   *
   * @param durationNanos the operation duration in nanoseconds
   * @param operationType the type of operation (e.g., "createTable", "evolveSchema")
   */
  @Override
  public void recordSchemaOperation(long durationNanos, String operationType) {
    var durationMs = TimeUnit.NANOSECONDS.toMillis(durationNanos);
    schemaOperationSensor.record(durationMs);
    schemaOperationCountSensor.record(1);

    // Also record to operation-specific sensors (timing + count)
    var timingSensor = operationTimingSensors.get(operationType);
    var countSensor = operationCountSensors.get(operationType);
    if (timingSensor != null) {
      timingSensor.record(durationMs);
    }
    if (countSensor != null) {
      countSensor.record(1);
    }
    if (timingSensor == null && countSensor == null) {
      LOG.log(
          System.Logger.Level.WARNING,
          "Unknown operation type: {0}. Available types: {1}",
          operationType,
          operationTimingSensors.keySet());
    }
  }

  /**
   * Records the number of records processed in a batch.
   *
   * @param recordCount the number of records
   */
  @Override
  public void recordBatchProcessed(int recordCount) {
    recordProcessingSensor.record(recordCount);
    batchSizeSensor.record(recordCount);
  }

  /**
   * Creates a timer to measure operation duration. Use with try-with-resources:
   *
   * <pre>
   * try (var timer = metrics.startJdbcQueryTimer()) {
   *   // execute query
   * }
   * </pre>
   */
  @Override
  public MetricTimer startJdbcQueryTimer() {
    return new MetricTimer(this::recordJdbcQuery);
  }

  /**
   * Creates a timer to measure JDBC query duration with operation type.
   *
   * <pre>
   * try (var timer = metrics.startJdbcQueryTimer("upsertWithMergeInto")) {
   *   // execute merge query
   * }
   * </pre>
   *
   * @param operationType the type of operation (e.g., "upsertWithMergeInto", "simpleInsert")
   */
  @Override
  public MetricTimer startJdbcQueryTimer(String operationType) {
    return new MetricTimer(duration -> recordJdbcQuery(duration, operationType));
  }

  /**
   * Creates a timer to measure schema operation duration. Use with try-with-resources:
   *
   * <pre>
   * try (var timer = metrics.startSchemaOperationTimer()) {
   *   // execute schema operation
   * }
   * </pre>
   */
  @Override
  public MetricTimer startSchemaOperationTimer() {
    return new MetricTimer(this::recordSchemaOperation);
  }

  /**
   * Creates a timer to measure schema operation duration with operation type.
   *
   * @param operationType the type of operation (e.g., "createTable", "evolveSchema")
   */
  @Override
  public MetricTimer startSchemaOperationTimer(String operationType) {
    return new MetricTimer(duration -> recordSchemaOperation(duration, operationType));
  }

  @Override
  public void close() {
    // Remove sensors from metrics registry
    if (jdbcQuerySensor != null) {
      metrics.removeSensor("jdbc-query");
    }
    if (jdbcQueryCountSensor != null) {
      metrics.removeSensor("jdbc-query-count-sensor");
    }
    if (schemaOperationSensor != null) {
      metrics.removeSensor("schema-operation");
    }
    if (schemaOperationCountSensor != null) {
      metrics.removeSensor("schema-operation-count-sensor");
    }
    if (recordProcessingSensor != null) {
      metrics.removeSensor("record-processing");
    }
    if (batchSizeSensor != null) {
      metrics.removeSensor("batch-size");
    }

    // Remove operation-specific sensors
    if (operationTimingSensors != null) {
      for (var operationType : operationTimingSensors.keySet()) {
        metrics.removeSensor("operation-" + operationType + "-timing");
      }
      for (var operationType : operationCountSensors.keySet()) {
        metrics.removeSensor("operation-" + operationType + "-count");
      }
    }

    LOG.log(System.Logger.Level.INFO, "Closed Ducklake metrics");
  }

  /**
   * Timer utility for measuring operation duration. Automatically records the duration when closed.
   */
  public static final class MetricTimer implements DucklakeMetricsInterface.MetricTimer {
    private final long startTimeNanos;
    private final java.util.function.LongConsumer recorder;

    private MetricTimer(java.util.function.LongConsumer recorder) {
      this.startTimeNanos = System.nanoTime();
      this.recorder = recorder;
    }

    @Override
    public void close() {
      var durationNanos = System.nanoTime() - startTimeNanos;
      recorder.accept(durationNanos);
    }
  }
}
