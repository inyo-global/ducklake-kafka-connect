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

import com.inyo.ducklake.ingestor.DucklakeWriter;
import com.inyo.ducklake.ingestor.DucklakeWriterConfig;
import java.sql.SQLException;
import org.duckdb.DuckDBConnection;

public final class DucklakeWriterFactory {

  private final DucklakeSinkConfig config;
  private final DuckDBConnection conn;
  private final DucklakeMetrics metrics;

  public DucklakeWriterFactory(DucklakeSinkConfig config, DuckDBConnection conn) {
    this(config, conn, null);
  }

  public DucklakeWriterFactory(
      DucklakeSinkConfig config, DuckDBConnection conn, DucklakeMetrics metrics) {
    this.config = config;
    this.metrics = metrics;
    try {
      this.conn = (DuckDBConnection) conn.duplicate();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates a DucklakeWriter for the specified topic.
   *
   * <p>This method handles topic-to-table mapping by:
   *
   * <ul>
   *   <li>Using explicit mappings from the configuration when available
   *   <li>Falling back to using the topic name as the table name when no mapping is configured
   * </ul>
   *
   * @param topic the Kafka topic name
   * @return a configured DucklakeWriter instance
   */
  public DucklakeWriter create(String topic) {
    final var topicsTables = config.getTopicToTableMap();
    // Use explicit mapping if available, otherwise use topic name as table name
    final var table = topicsTables.getOrDefault(topic, topic);

    final var idCols = config.getTableIdColumns(table);
    final var partitionExprs = config.getTablePartitionByExpressions(table);
    final var autoCreate = config.getTableAutoCreate(table);

    DucklakeWriterConfig writerConfig =
        new DucklakeWriterConfig(table, autoCreate, idCols, partitionExprs);

    return new DucklakeWriter(conn, writerConfig, metrics);
  }
}
