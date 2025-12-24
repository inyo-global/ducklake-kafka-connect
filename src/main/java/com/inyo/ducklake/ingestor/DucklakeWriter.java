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
package com.inyo.ducklake.ingestor;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DucklakeWriter implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(DucklakeWriter.class);

  private final DuckDBConnection connection;
  private final DucklakeWriterConfig config;
  private final DucklakeTableManager tableManager;

  public DucklakeWriter(DuckDBConnection connection, DucklakeWriterConfig config) {
    if (connection == null) {
      throw new IllegalArgumentException("DuckDBConnection cannot be null");
    }
    this.config = config;
    try {
      // Defensive duplicate so internal state is isolated from caller
      this.connection = (DuckDBConnection) connection.duplicate();
    } catch (SQLException e) {
      throw new RuntimeException("Failed to duplicate DuckDB connection for writer", e);
    }
    this.tableManager = new DucklakeTableManager(this.connection, config);
  }

  // Ensure schema (create/evolve) then insert rows
  public void write(VectorSchemaRoot root) {
    if (root == null || root.getRowCount() == 0) {
      LOG.debug("No data to write");
      return;
    }
    try {
      var schema = root.getSchema();
      LOG.info("Writing {} rows to table {}", root.getRowCount(), config.destinationTable());

      boolean tableExistedBefore = tableManager.ensureTable(schema);

      LOG.debug("ensureTable completed for {}", config.destinationTable());
      insertData(root, tableExistedBefore);
      LOG.debug("Write completed for table {}", config.destinationTable());
    } catch (SQLException e) {
      LOG.error("Failed during write to {}: {}", config.destinationTable(), e.getMessage());
      throw new RuntimeException("Failed to write data to " + config.destinationTable(), e);
    }
  }

  private void insertData(VectorSchemaRoot root, boolean tableExistedBefore) throws SQLException {
    var fields = root.getSchema().getFields();
    var pkCols = config.tableIdColumns();

    if (pkCols.length > 0 && tableExistedBefore) {
      // Table existed before and has PK columns - use MERGE INTO for upsert behavior
      upsertWithMergeInto(root, fields, pkCols);
    } else {
      // New table or no PK columns - use simple INSERT to avoid DuckDB MERGE bugs with partition-by
      simpleInsert(root, fields);
    }
  }

  private void upsertWithMergeInto(VectorSchemaRoot root, List<Field> fields, String[] pkCols)
      throws SQLException {
    var tableQuoted = "lake.main." + SqlIdentifierUtil.quote(config.destinationTable());
    var tempTable = "new_data_" + java.util.UUID.randomUUID().toString().replace("-", "");

    var allocator = root.getFieldVectors().get(0).getAllocator();
    var reader = RootArrowReader.fromRoot(allocator, root);
    try (var arrayStream = ArrowArrayStream.allocateNew(allocator)) {
      Data.exportArrayStream(allocator, reader, arrayStream);
      LOG.debug("Registering Arrow stream as view {} with {} rows", tempTable, root.getRowCount());
      connection.registerArrowStream(tempTable, arrayStream);

      // Build USING clause for PK
      var usingClause =
          java.util.Arrays.stream(pkCols)
              .map(SqlIdentifierUtil::quote)
              .collect(Collectors.joining(", "));

      var pkSet =
          java.util.Arrays.stream(pkCols)
              .map(c -> c.toLowerCase(java.util.Locale.ROOT))
              .collect(java.util.stream.Collectors.toSet());
      var nonKey =
          fields.stream()
              .filter(f -> !pkSet.contains(f.getName().toLowerCase(java.util.Locale.ROOT)))
              .toList();
      var updateSetClause =
          nonKey.isEmpty()
              ? null
              : nonKey.stream()
                  .map(
                      f ->
                          SqlIdentifierUtil.quote(f.getName())
                              + " = "
                              + tempTable
                              + "."
                              + SqlIdentifierUtil.quote(f.getName()))
                  .collect(Collectors.joining(", "));
      var insertCols =
          fields.stream()
              .map(f -> tempTable + "." + SqlIdentifierUtil.quote(f.getName()))
              .collect(Collectors.joining(", "));
      var targetCols =
          fields.stream()
              .map(f -> SqlIdentifierUtil.quote(f.getName()))
              .collect(Collectors.joining(", "));
      var sql = new StringBuilder();
      sql.append("MERGE INTO ")
          .append(tableQuoted)
          .append(" USING ")
          .append(tempTable)
          .append(" USING (")
          .append(usingClause)
          .append(") ");
      if (updateSetClause != null) {
        sql.append("WHEN MATCHED THEN UPDATE SET ").append(updateSetClause).append(" ");
      }
      // Use explicit target column list to avoid positional inserts. Map by name instead of by
      // position.
      sql.append("WHEN NOT MATCHED THEN INSERT (")
          .append(targetCols)
          .append(") VALUES (")
          .append(insertCols)
          .append(")");

      LOG.debug("Executing MERGE: {}", sql);
      try (var ps = connection.prepareStatement(sql.toString())) {
        int affected = ps.executeUpdate();
        LOG.info("MERGE affected {} rows on table {}", affected, config.destinationTable());
      }
    }
    try (var dropPs = connection.prepareStatement("DROP VIEW IF EXISTS " + tempTable)) {
      dropPs.executeUpdate();
      LOG.debug("Dropped temp view {}", tempTable);
    }
  }

  private void simpleInsert(VectorSchemaRoot root, List<Field> fields) throws SQLException {
    var tableQuoted = "lake.main." + SqlIdentifierUtil.quote(config.destinationTable());
    var tempTable = "new_data_" + java.util.UUID.randomUUID().toString().replace("-", "");

    var allocator = root.getFieldVectors().get(0).getAllocator();
    var reader = RootArrowReader.fromRoot(allocator, root);
    try (var arrayStream = ArrowArrayStream.allocateNew(allocator)) {
      Data.exportArrayStream(allocator, reader, arrayStream);
      LOG.debug(
          "Registering Arrow stream as view {} with {} rows for simple insert",
          tempTable,
          root.getRowCount());
      connection.registerArrowStream(tempTable, arrayStream);

      // Build simple INSERT INTO ... SELECT FROM tempTable
      var columnNames =
          fields.stream()
              .map(f -> SqlIdentifierUtil.quote(f.getName()))
              .collect(java.util.stream.Collectors.joining(", "));
      var selectColumns =
          fields.stream()
              .map(f -> tempTable + "." + SqlIdentifierUtil.quote(f.getName()))
              .collect(java.util.stream.Collectors.joining(", "));

      var sql = new StringBuilder();
      sql.append("INSERT INTO ")
          .append(tableQuoted)
          .append(" (")
          .append(columnNames)
          .append(") SELECT ")
          .append(selectColumns)
          .append(" FROM ")
          .append(tempTable);

      LOG.debug("Executing simple INSERT: {}", sql);
      try (var ps = connection.prepareStatement(sql.toString())) {
        int affected = ps.executeUpdate();
        LOG.debug(
            "Simple INSERT affected {} rows on table {}", affected, config.destinationTable());
      }
    }
    // Drop the temp view to avoid memory leaks
    try (var dropPs = connection.prepareStatement("DROP VIEW IF EXISTS " + tempTable)) {
      dropPs.executeUpdate();
      LOG.debug("Dropped temp view {}", tempTable);
    }
  }

  @Override
  public void close() {
    try {
      tableManager.close();
    } catch (Exception e) {
      LOG.warn("Failed to close table manager: {}", e.getMessage());
    }
    try {
      connection.close();
    } catch (SQLException e) {
      LOG.warn("Failed to close writer connection: {}", e.getMessage());
    }
  }
}
