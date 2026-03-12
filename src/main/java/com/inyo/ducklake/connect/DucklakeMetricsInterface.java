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

/** Interface to abstract Ducklake metrics operations so tests can mock metrics behavior. */
public interface DucklakeMetricsInterface extends AutoCloseable {

  void recordJdbcQuery(long durationNanos);

  void recordJdbcQuery(long durationNanos, String operationType);

  void recordSchemaOperation(long durationNanos);

  void recordSchemaOperation(long durationNanos, String operationType);

  void recordBatchProcessed(int recordCount);

  MetricTimer startJdbcQueryTimer();

  MetricTimer startJdbcQueryTimer(String operationType);

  MetricTimer startSchemaOperationTimer();

  MetricTimer startSchemaOperationTimer(String operationType);

  /** Simple marker interface for timers returned by the metrics implementation. */
  interface MetricTimer extends AutoCloseable {}
}
