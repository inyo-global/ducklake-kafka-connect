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

  public DucklakeWriterFactory(DucklakeSinkConfig config, DuckDBConnection conn) {
    this.config = config;
    try {
      this.conn = (DuckDBConnection) conn.duplicate();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public DucklakeWriter create(String topic) {
    final var topicsTables = config.getTopicToTableMap();
    final var table = topicsTables.getOrDefault(topic, topic);

    final var idCols = config.getTableIdColumns(table);
    final var partitionCols = config.getTablePartitionByColumns(table);
    final var autoCreate = config.getTableAutoCreate(table);

    DucklakeWriterConfig writerConfig =
        new DucklakeWriterConfig(table, autoCreate, idCols, partitionCols);

    return new DucklakeWriter(conn, writerConfig);
  }
}
