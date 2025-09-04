package com.inyo.ducklake.ingestor;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;

import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Responsible for all schema/table management operations:
 *  - Check existence
 *  - Create table
 *  - Evolve (add new columns)
 *  - Validate type compatibility
 */
public final class DucklakeTableManager {

    private static final System.Logger LOG = System.getLogger(DucklakeTableManager.class.getName());

    private final DuckDBConnection connection;
    private final DucklakeWriterConfig config;
    private Map<String, ColumnMeta> cachedMeta; // lowercase column name -> meta
    private static final Pattern IDENT_PATTERN = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

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
            LOG.log(System.Logger.Level.WARNING, "Failed to close duplicated DuckDB connection: {0}", e.getMessage());
        }
    }

    /**
     * Ensures that the table exists and reflects (at least) the columns of the provided Arrow schema.
     * Creates it if allowed; evolves (ADD COLUMN) new columns; validates existing column types.
     */
    public void ensureTable(Schema arrowSchema) throws SQLException {
        String table = config.destinationTable();
        if (!tableExists(table)) {
            if (!config.autoCreateTable()) {
                throw new IllegalStateException("Table does not exist and auto-create is disabled: " + table);
            }
            createTable(arrowSchema);
            LOG.log(System.Logger.Level.INFO, "Table created: {0}", table);
        } else {
            evolveTableSchema(arrowSchema);
        }
    }

    public boolean tableExists(String table) throws SQLException {
        final String sql = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main' AND table_name = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, table);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1) > 0;
                }
            }
        }
        return false;
    }

    private String safeIdentifier(String raw) {
        if (raw == null || !IDENT_PATTERN.matcher(raw).matches()) {
            throw new IllegalArgumentException("Invalid identifier: " + raw);
        }
        return raw;
    }

    @SuppressFBWarnings( value = "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE", justification = "Identifiers sanitized via safeIdentifier; PreparedStatement cannot parameterize DDL identifiers." )
    private void createTable(Schema arrowSchema) throws SQLException {
        // Validate table & column identifiers early (prepared statements cannot parameterize DDL identifiers)
        String tableName = safeIdentifier(config.destinationTable());
        for (Field f : arrowSchema.getFields()) {
            safeIdentifier(f.getName());
        }
        String cols = arrowSchema.getFields().stream()
                .map(f -> quote(f.getName()) + " " + toDuckDBType(f.getType()))
                .collect(Collectors.joining(", "));
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE ").append(quote(tableName)).append(" (");
        ddl.append(cols);
        String[] pkCols = config.tableIdColumns();
        if (pkCols.length > 0) {
            for (String pk : pkCols) { safeIdentifier(pk); }
            String pk = Arrays.stream(pkCols).map(this::quote).collect(Collectors.joining(","));
            ddl.append(", PRIMARY KEY (").append(pk).append(")");
        }
        ddl.append(")");
        // SpotBugs SQL: Using Statement with sanitized identifiers only (no untrusted user input).
        try (Statement st = connection.createStatement()) {
            st.execute(ddl.toString());
        }
        // Populate cache directly from provided schema (avoid PRAGMA round-trip)
        Map<String, ColumnMeta> map = new HashMap<>();
        for (Field f : arrowSchema.getFields()) {
            map.put(f.getName().toLowerCase(Locale.ROOT), new ColumnMeta(f.getName(), toDuckDBType(f.getType())));
        }
        cachedMeta = map;
    }

    private void evolveTableSchema(Schema arrowSchema) throws SQLException {
        Map<String, ColumnMeta> existing = loadExistingTableMeta();
        List<Field> fields = arrowSchema.getFields();
        List<Field> newColumns = new ArrayList<>();
        for (Field field : fields) {
            String colNameLower = field.getName().toLowerCase(Locale.ROOT);
            ColumnMeta meta = existing.get(colNameLower);
            if (meta == null) {
                newColumns.add(field);
            } else {
                String expectedDuck = toDuckDBType(field.getType());
                if (!meta.type.equalsIgnoreCase(expectedDuck)) {
                    TypeEvolutionDecision decision = evaluateTypeEvolution(meta.type, expectedDuck);
                    switch (decision) {
                        case COMPATIBLE_KEEP -> { /* nothing */ }
                        case UPGRADE -> performTypeUpgrade(field.getName(), expectedDuck);
                        case INCOMPATIBLE -> throw new IllegalStateException("Incompatible type for column " + field.getName() + ": existing=" + meta.type + ", expected=" + expectedDuck);
                    }
                }
            }
        }
        if (!newColumns.isEmpty()) {
            for (Field nf : newColumns) {
                String newType = toDuckDBType(nf.getType());
                String ddl = "ALTER TABLE " + quote(config.destinationTable()) + " ADD COLUMN " + quote(nf.getName()) + " " + newType;
                LOG.log(System.Logger.Level.INFO, "Adding new column: {0}", ddl);
                try (Statement st = connection.createStatement()) {
                    st.execute(ddl);
                }
                if (cachedMeta != null) {
                    cachedMeta.put(nf.getName().toLowerCase(Locale.ROOT), new ColumnMeta(nf.getName(), newType));
                }
            }
        }
    }

    private enum TypeEvolutionDecision { COMPATIBLE_KEEP, UPGRADE, INCOMPATIBLE }

    private TypeEvolutionDecision evaluateTypeEvolution(String existing, String expected) {
        String e = existing.toUpperCase(Locale.ROOT);
        String ex = expected.toUpperCase(Locale.ROOT);
        if (e.equals(ex)) return TypeEvolutionDecision.COMPATIBLE_KEEP;
        // JSON only matches JSON (no evolution between JSON and others)
        if (e.equals("JSON") || ex.equals("JSON")) {
            return TypeEvolutionDecision.INCOMPATIBLE;
        }
        List<String> intOrder = List.of("TINYINT","SMALLINT","INTEGER","BIGINT");
        if (intOrder.contains(e) && intOrder.contains(ex)) {
            int idxE = intOrder.indexOf(e);
            int idxEx = intOrder.indexOf(ex);
            if (idxEx > idxE) return TypeEvolutionDecision.UPGRADE; // widening
            return TypeEvolutionDecision.COMPATIBLE_KEEP; // equal or narrower requested
        }
        // FLOAT / DOUBLE promotion
        if (e.equals("FLOAT") && ex.equals("DOUBLE")) return TypeEvolutionDecision.UPGRADE;
        // Narrowing double->float treat as keep (do not downgrade) but still compatible
        if (e.equals("DOUBLE") && ex.equals("FLOAT")) return TypeEvolutionDecision.COMPATIBLE_KEEP;
        // Other types must match exactly
        return TypeEvolutionDecision.INCOMPATIBLE;
    }

    private void performTypeUpgrade(String columnName, String newType) throws SQLException {
        String ddl = "ALTER TABLE " + quote(config.destinationTable()) + " ALTER COLUMN " + quote(columnName) + " SET DATA TYPE " + newType;
        LOG.log(System.Logger.Level.INFO, "Upgrading column type: {0}", ddl);
        try (Statement st = connection.createStatement()) {
            st.execute(ddl);
        }
        if (cachedMeta != null) {
            cachedMeta.put(columnName.toLowerCase(Locale.ROOT), new ColumnMeta(columnName, newType));
        }
    }

    private Map<String, ColumnMeta> loadExistingTableMeta() throws SQLException {
        if (cachedMeta != null) {
            return cachedMeta;
        }
        Map<String, ColumnMeta> map = new HashMap<>();
        final String pragma = "PRAGMA table_info(" + quote(config.destinationTable()) + ")";
        try (Statement st = connection.createStatement(); ResultSet rs = st.executeQuery(pragma)) {
            while (rs.next()) {
                String name = rs.getString("name");
                String type = rs.getString("type");
                map.put(name.toLowerCase(Locale.ROOT), new ColumnMeta(name, type));
            }
        }
        cachedMeta = map;
        return cachedMeta;
    }


    boolean isMetadataCached() { return cachedMeta != null; }
    Set<String> cachedColumnNames() { return cachedMeta == null ? Set.of() : new HashSet<>(cachedMeta.keySet()); }

    private record ColumnMeta(String name, String type) {}

    public String toDuckDBType(ArrowType type) {
        if (type instanceof ArrowType.Int i) {
            return switch (i.getBitWidth()) { case 8 -> "TINYINT"; case 16 -> "SMALLINT"; case 32 -> "INTEGER"; case 64 -> "BIGINT"; default -> throw new IllegalArgumentException("Unsupported int bit width: " + i.getBitWidth());};
        } else if (type instanceof ArrowType.FloatingPoint fp) {
            return switch (fp.getPrecision()) { case SINGLE -> "FLOAT"; case DOUBLE -> "DOUBLE"; default -> throw new IllegalArgumentException("Unsupported floating precision: " + fp.getPrecision());};
        } else if (type instanceof ArrowType.Bool) {
            return "BOOLEAN";
        } else if (type instanceof ArrowType.Utf8) {
            return "VARCHAR";
        } else if (type instanceof ArrowType.Binary) {
            return "BLOB";
        } else if (type instanceof ArrowType.Struct || type instanceof ArrowType.List || type instanceof ArrowType.Map) {
            return "JSON"; // store complex types as JSON
        }
        throw new IllegalArgumentException("Unsupported Arrow type: " + type);
    }

    public String quote(String identifier) {
        return identifier.matches("[a-zA-Z_][a-zA-Z0-9_]*") ? identifier : '"' + identifier.replace("\"", "\"\"") + '"';
    }
}
