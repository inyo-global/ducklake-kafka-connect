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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;

/**
 * Responsible for all schema/table management operations: - Check existence - Create table - Evolve
 * (add new columns) - Validate type compatibility
 */
public final class DucklakeTableManager {

  private static final System.Logger LOG = System.getLogger(DucklakeTableManager.class.getName());
  private static final Object LOCK = new Object();

  private final DuckDBConnection connection;
  private final DucklakeWriterConfig config;

  public DucklakeTableManager(DuckDBConnection connection, DucklakeWriterConfig config) {
    this.config = config;
    try {
      // Defensive copy to avoid exposing external mutable connection instance
      this.connection = (DuckDBConnection) connection.duplicate();
    } catch (SQLException e) {
      throw new RuntimeException("Failed to duplicate DuckDB connection", e);
    }
  }

  public void close() {
    try {
      connection.close();
    } catch (SQLException e) {
      LOG.log(
          System.Logger.Level.WARNING,
          "Failed to close duplicated DuckDB connection: {0}",
          e.getMessage());
    }
  }

  /**
   * Ensures that the table exists and reflects (at least) the columns of the provided Arrow schema.
   * Creates it if allowed; evolves (ADD COLUMN) new columns; validates existing column types.
   *
   * @param arrowSchema the Arrow schema to ensure
   * @return true if the table existed before this operation, false if it was created
   */
  public boolean ensureTable(Schema arrowSchema) throws SQLException {
    synchronized (LOCK) {
      final var table = config.destinationTable();
      final var tableExisted = tableExists(table);
      if (!tableExisted) {
        if (!config.autoCreateTable()) {
          throw new IllegalStateException(
              "Table does not exist and auto-create is disabled: " + table);
        }
        createTable(arrowSchema);
        LOG.log(System.Logger.Level.INFO, "Table created: {0}", table);
      } else {
        evolveTableSchema(arrowSchema);
      }
      return tableExisted;
    }
  }

  private String qualifiedTableRef() {
    // Catalog alias 'lake', default schema 'main', quote only the table identifier
    return "lake.main." + SqlIdentifierUtil.quote(config.destinationTable());
  }

  public boolean tableExists(String table) {
    // Use PRAGMA table_info to check existence within attached catalog 'lake'
    final var tableName = "lake.main." + SqlIdentifierUtil.quote(config.destinationTable());
    final var sql = String.format("pragma table_info(%s)", tableName);
    try (PreparedStatement ps = connection.prepareStatement(sql)) {
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    } catch (SQLException e) {
      // If table does not exist, DuckDB may raise a Catalog Error; treat as non-existent
      LOG.log(
          System.Logger.Level.DEBUG,
          "tableExists({0}) via PRAGMA failed: {1}",
          table,
          e.getMessage());
      return false;
    }
  }

  @SuppressFBWarnings(
      value = "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE",
      justification =
          "Identifiers quoted via SqlIdentifierUtil.quote; "
              + "PreparedStatement cannot parameterize DDL identifiers.")
  private void createTable(Schema arrowSchema) throws SQLException {
    var cols =
        arrowSchema.getFields().stream()
            .map(f -> SqlIdentifierUtil.quote(f.getName()) + " " + toDuckDBType(f.getType()))
            .collect(Collectors.joining(", "));
    var ddl = new StringBuilder();
    ddl.append("CREATE TABLE ")
        .append("lake.main.")
        .append(SqlIdentifierUtil.quote(config.destinationTable()))
        .append(" (");
    ddl.append(cols);
    // PRIMARY KEY constraints are not supported in DuckLake, removing the constraint definition
    ddl.append(")");

    try (Statement st = connection.createStatement()) {
      st.execute(ddl.toString());
    }

    // Set partitioning after table creation using ALTER TABLE SET PARTITIONED BY
    if (config.partitionByExpressions().length > 0) {
      final var partitionExprs = String.join(", ", config.partitionByExpressions());
      final var alterDdl =
          "ALTER TABLE " + qualifiedTableRef() + " SET PARTITIONED BY (" + partitionExprs + ")";
      LOG.log(System.Logger.Level.INFO, "Setting table partitioning: {0}", alterDdl);
      try (Statement st = connection.createStatement()) {
        st.execute(alterDdl);
      } catch (SQLException e) {
        LOG.log(System.Logger.Level.ERROR, "Failed to set table partitioning", e);
        throw e;
      }
    }
  }

  @SuppressFBWarnings(
      value = "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE",
      justification = "DDL evolve validated: identifiers quoted via SqlIdentifierUtil.quote.")
  private void evolveTableSchema(Schema arrowSchema) throws SQLException {
    final var existing = loadExistingTableMeta();
    final var fields = arrowSchema.getFields();
    final var newColumns = new ArrayList<Field>();
    for (final var field : fields) {
      final var colNameLower = field.getName().toLowerCase(Locale.ROOT);
      final var meta = existing.get(colNameLower);
      if (meta == null) {
        newColumns.add(field);
      } else {
        final var expectedDuck = toDuckDBType(field.getType());
        if (!meta.equalsIgnoreCase(expectedDuck)) {
          final var decision = evaluateTypeEvolution(meta, expectedDuck);
          switch (decision) {
            case COMPATIBLE_KEEP -> {}
            case UPGRADE -> performTypeUpgrade(field.getName(), expectedDuck);
            case INCOMPATIBLE ->
                throw new IllegalStateException(
                    "Incompatible type for column "
                        + field.getName()
                        + ": existing="
                        + meta
                        + ", expected="
                        + expectedDuck);
          }
        }
      }
    }
    if (newColumns.isEmpty()) {
      return;
    }
    for (final var nf : newColumns) {
      final var newType = toDuckDBType(nf.getType());
      final var ddl =
          "ALTER TABLE "
              + qualifiedTableRef()
              + " ADD COLUMN "
              + SqlIdentifierUtil.quote(nf.getName())
              + " "
              + newType;
      LOG.log(System.Logger.Level.INFO, "Adding new column: {0}", ddl);
      try (final var st = connection.createStatement()) {
        st.execute(ddl);
      } catch (SQLException e) {
        LOG.log(
            System.Logger.Level.ERROR,
            "Failed to add new column "
                + nf.getName()
                + " of type "
                + newType
                + ": "
                + " to table: "
                + qualifiedTableRef()
                + e.getMessage(),
            e);
        throw e;
      }
    }
  }

  private enum TypeEvolutionDecision {
    COMPATIBLE_KEEP,
    UPGRADE,
    INCOMPATIBLE
  }

  private TypeEvolutionDecision evaluateTypeEvolution(String existing, String expected) {
    String e = existing.toUpperCase(Locale.ROOT);
    String ex = expected.toUpperCase(Locale.ROOT);
    if (e.equals(ex)) return TypeEvolutionDecision.COMPATIBLE_KEEP;
    if (e.equals("JSON") || ex.equals("JSON")) {
      return TypeEvolutionDecision.INCOMPATIBLE;
    }
    List<String> intOrder = List.of("TINYINT", "SMALLINT", "INTEGER", "BIGINT");
    if (intOrder.contains(e) && intOrder.contains(ex)) {
      int idxE = intOrder.indexOf(e);
      int idxEx = intOrder.indexOf(ex);
      if (idxEx > idxE) return TypeEvolutionDecision.UPGRADE;
      return TypeEvolutionDecision.COMPATIBLE_KEEP;
    }
    if (e.equals("FLOAT") && ex.equals("DOUBLE")) return TypeEvolutionDecision.UPGRADE;
    if (e.equals("DOUBLE") && ex.equals("FLOAT")) return TypeEvolutionDecision.COMPATIBLE_KEEP;
    return TypeEvolutionDecision.INCOMPATIBLE;
  }

  @SuppressFBWarnings(
      value = "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE",
      justification = "Identifiers quoted via SqlIdentifierUtil.quote.")
  private void performTypeUpgrade(String columnName, String newType) throws SQLException {
    String ddl =
        "ALTER TABLE "
            + qualifiedTableRef()
            + " ALTER COLUMN "
            + SqlIdentifierUtil.quote(columnName)
            + " SET DATA TYPE "
            + newType;
    LOG.log(System.Logger.Level.INFO, "Upgrading column type: {0}", ddl);
    try (Statement st = connection.createStatement()) {
      st.execute(ddl);
    }
  }

  private Map<String, String> loadExistingTableMeta() throws SQLException {
    Map<String, String> map = new HashMap<>();
    final String sql = String.format("PRAGMA table_info(%s)", qualifiedTableRef());
    try (Statement st = connection.createStatement()) {
      try (ResultSet rs = st.executeQuery(sql)) {
        while (rs.next()) {
          String name = rs.getString("name");
          String type = rs.getString("type");
          map.put(name.toLowerCase(Locale.ROOT), type);
        }
      }
    }
    return map;
  }

  public String toDuckDBType(ArrowType type) {
    if (type instanceof ArrowType.Int i) {
      return switch (i.getBitWidth()) {
        case ArrowTypeConstants.INT8_BIT_WIDTH -> "TINYINT";
        case ArrowTypeConstants.INT16_BIT_WIDTH -> "SMALLINT";
        case ArrowTypeConstants.INT32_BIT_WIDTH -> "INTEGER";
        case ArrowTypeConstants.INT64_BIT_WIDTH -> "BIGINT";
        default ->
            throw new IllegalArgumentException("Unsupported int bit width: " + i.getBitWidth());
      };
    } else if (type instanceof ArrowType.FloatingPoint fp) {
      return switch (fp.getPrecision()) {
        case SINGLE -> "FLOAT";
        case DOUBLE -> "DOUBLE";
        default ->
            throw new IllegalArgumentException(
                "Unsupported floating precision: " + fp.getPrecision());
      };
    } else if (type instanceof ArrowType.Bool) {
      return "BOOLEAN";
    } else if (type instanceof ArrowType.Utf8) {
      return "VARCHAR";
    } else if (type instanceof ArrowType.Binary) {
      return "BLOB";
    } else if (type instanceof ArrowType.Timestamp timestamp) {
      // Suporte para timestamps com diferentes unidades de tempo
      return switch (timestamp.getUnit()) {
        case SECOND, MILLISECOND, MICROSECOND, NANOSECOND -> "TIMESTAMP";
      };
    } else if (type instanceof ArrowType.Date date) {
      return switch (date.getUnit()) {
        case DAY -> "DATE";
        case MILLISECOND -> "DATE";
      };
    } else if (type instanceof ArrowType.Time time) {
      return switch (time.getUnit()) {
        case SECOND, MILLISECOND, MICROSECOND, NANOSECOND -> "TIME";
      };
    } else if (type instanceof ArrowType.Struct
        || type instanceof ArrowType.List
        || type instanceof ArrowType.Map) {
      return "JSON";
    }
    throw new IllegalArgumentException("Unsupported Arrow type: " + type);
  }
}
