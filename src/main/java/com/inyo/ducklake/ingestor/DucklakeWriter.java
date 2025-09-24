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

public final class DucklakeWriter implements AutoCloseable {

  private static final System.Logger LOG = System.getLogger(DucklakeWriter.class.getName());

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
      LOG.log(System.Logger.Level.DEBUG, "No data to write");
      return;
    }
    try {
      var schema = root.getSchema();
      LOG.log(
          System.Logger.Level.INFO,
          "Writing {0} rows to table {1}",
          root.getRowCount(),
          config.destinationTable());
      tableManager.ensureTable(schema);
      LOG.log(
          System.Logger.Level.DEBUG, "ensureTable completed for {0}", config.destinationTable());
      insertData(root);
      LOG.log(System.Logger.Level.INFO, "Write completed for table {0}", config.destinationTable());
    } catch (SQLException e) {
      LOG.log(
          System.Logger.Level.ERROR,
          "Failed during write to {0}: {1}",
          config.destinationTable(),
          e.getMessage());
      throw new RuntimeException("Failed to write data to " + config.destinationTable(), e);
    }
  }

  private void insertData(VectorSchemaRoot root) throws SQLException {
    var fields = root.getSchema().getFields();
    var pkCols = config.tableIdColumns();
    // If there are PK columns, always use MERGE INTO logic
    upsertWithMergeInto(root, fields, pkCols);
  }

  private void upsertWithMergeInto(VectorSchemaRoot root, List<Field> fields, String[] pkCols)
      throws SQLException {
    var tableQuoted = "lake.main." + SqlIdentifierUtil.quote(config.destinationTable());
    var tempTable = "new_data_" + java.util.UUID.randomUUID().toString().replace("-", "");

    var allocator = root.getFieldVectors().get(0).getAllocator();
    var reader = RootArrowReader.fromRoot(allocator, root);
    try (var arrayStream = ArrowArrayStream.allocateNew(allocator)) {
      Data.exportArrayStream(allocator, reader, arrayStream);
      LOG.log(
          System.Logger.Level.DEBUG,
          "Registering Arrow stream as view {0} with {1} rows",
          new Object[] {tempTable, root.getRowCount()});
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
      sql.append("WHEN NOT MATCHED THEN INSERT VALUES (").append(insertCols).append(")");

      LOG.log(System.Logger.Level.DEBUG, "Executing MERGE: {0}", sql.toString());
      try (var ps = connection.prepareStatement(sql.toString())) {
        int affected = ps.executeUpdate();
        LOG.log(
            System.Logger.Level.INFO,
            "MERGE affected {0} rows on table {1}",
            affected,
            config.destinationTable());
      }
    }
    try (var dropPs = connection.prepareStatement("DROP VIEW IF EXISTS " + tempTable)) {
      dropPs.executeUpdate();
      LOG.log(System.Logger.Level.DEBUG, "Dropped temp view {0}", tempTable);
    }
  }

  @Override
  public void close() {
    try {
      tableManager.close();
    } catch (Exception e) {
      LOG.log(System.Logger.Level.WARNING, "Failed to close table manager: {0}", e.getMessage());
    }
    try {
      connection.close();
    } catch (SQLException e) {
      LOG.log(
          System.Logger.Level.WARNING, "Failed to close writer connection: {0}", e.getMessage());
    }
  }
}
