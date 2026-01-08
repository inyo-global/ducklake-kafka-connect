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
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for all schema/table management operations: - Check existence - Create table - Evolve
 * (add new columns) - Validate type compatibility
 */
public final class DucklakeTableManager {

  private static final Logger LOG = LoggerFactory.getLogger(DucklakeTableManager.class);

  // Per-table locks to allow concurrent operations on different tables
  private static final ConcurrentHashMap<String, Object> TABLE_LOCKS = new ConcurrentHashMap<>();

  // Cache of tables that have been verified to exist (avoids repeated PRAGMA queries)
  private static final ConcurrentHashMap<String, Boolean> VERIFIED_TABLES =
      new ConcurrentHashMap<>();

  // Cache of known columns per table (lowercase column names) to avoid repeated PRAGMA queries
  private static final ConcurrentHashMap<String, Set<String>> KNOWN_COLUMNS =
      new ConcurrentHashMap<>();

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
      LOG.warn("Failed to close duplicated DuckDB connection: {}", e.getMessage());
    }
  }

  /**
   * Ensures that the table exists and reflects (at least) the columns of the provided Arrow schema.
   * Creates it if allowed; evolves (ADD COLUMN) new columns; validates existing column types.
   *
   * <p>Uses caching to avoid repeated metadata queries for tables that have already been verified.
   *
   * @param arrowSchema the Arrow schema to ensure
   * @return true if the table existed before this operation, false if it was created
   */
  public boolean ensureTable(Schema arrowSchema) throws SQLException {
    final var table = config.destinationTable();
    final var tableKey = table.toLowerCase(Locale.ROOT);

    // Fast path: if table is already verified, only check for schema evolution
    Boolean cachedExists = VERIFIED_TABLES.get(tableKey);
    if (cachedExists != null && cachedExists) {
      // Table exists and was verified before - only check for new columns
      evolveTableSchemaIfNeeded(arrowSchema);
      return true;
    }

    // Slow path: first time seeing this table, need full verification
    Object tableLock = TABLE_LOCKS.computeIfAbsent(tableKey, k -> new Object());

    synchronized (tableLock) {
      // Double-check after acquiring lock
      cachedExists = VERIFIED_TABLES.get(tableKey);
      if (cachedExists != null && cachedExists) {
        evolveTableSchemaIfNeeded(arrowSchema);
        return true;
      }

      final var tableExisted = tableExists(table);
      if (!tableExisted) {
        if (!config.autoCreateTable()) {
          throw new IllegalStateException(
              "Table does not exist and auto-create is disabled: " + table);
        }
        createTable(arrowSchema);
        LOG.info("Table created: {}", table);
      } else {
        evolveTableSchema(arrowSchema);
      }

      // Mark table as verified
      VERIFIED_TABLES.put(tableKey, true);
      return tableExisted;
    }
  }

  /**
   * Evolves table schema if there are new columns or type changes needed. Always calls
   * evolveTableSchema since type upgrades (INTEGER→BIGINT, FLOAT→DOUBLE) need to be checked even
   * for existing columns.
   */
  private void evolveTableSchemaIfNeeded(Schema arrowSchema) throws SQLException {
    // Always call evolveTableSchema - it handles:
    // 1. Adding new columns
    // 2. Upgrading types (INTEGER→BIGINT, FLOAT→DOUBLE)
    // 3. Rejecting incompatible type changes
    // The method itself short-circuits when no changes are needed
    evolveTableSchema(arrowSchema);
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
      LOG.debug("tableExists({}) via PRAGMA failed: {}", table, e.getMessage());
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
      LOG.info("Setting table partitioning: {}", alterDdl);
      try (Statement st = connection.createStatement()) {
        st.execute(alterDdl);
      } catch (SQLException e) {
        LOG.error("Failed to set table partitioning", e);
        throw e;
      }
    }

    // Cache the columns we just created
    final var tableKey = config.destinationTable().toLowerCase(Locale.ROOT);
    Set<String> columnSet =
        arrowSchema.getFields().stream()
            .map(f -> f.getName().toLowerCase(Locale.ROOT))
            .collect(Collectors.toCollection(HashSet::new));
    KNOWN_COLUMNS.put(tableKey, ConcurrentHashMap.newKeySet());
    KNOWN_COLUMNS.get(tableKey).addAll(columnSet);
  }

  @SuppressFBWarnings(
      value = "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE",
      justification = "DDL evolve validated: identifiers quoted via SqlIdentifierUtil.quote.")
  private void evolveTableSchema(Schema arrowSchema) throws SQLException {
    final var tableKey = config.destinationTable().toLowerCase(Locale.ROOT);
    final var existing = loadExistingTableMeta();

    // Update the column cache with existing columns from DB
    Set<String> knownCols =
        KNOWN_COLUMNS.computeIfAbsent(tableKey, k -> ConcurrentHashMap.newKeySet());
    knownCols.addAll(existing.keySet());

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
      LOG.info("Adding new column: {}", ddl);
      try (final var st = connection.createStatement()) {
        st.execute(ddl);
        // Add newly created column to the cache
        knownCols.add(nf.getName().toLowerCase(Locale.ROOT));
      } catch (SQLException e) {
        LOG.error(
            "Failed to add new column {} of type {} to table: {}",
            nf.getName(),
            newType,
            qualifiedTableRef(),
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
    LOG.info("Upgrading column type: {}", ddl);
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
