package com.inyo.ducklake.ingestor;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;

import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
            Schema schema = root.getSchema();
            tableManager.ensureTable(schema);
            insertData(root);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to write data to " + config.destinationTable(), e);
        }
    }

    private void insertData(VectorSchemaRoot root) throws SQLException {
        List<Field> fields = root.getSchema().getFields();
        String columnList = fields.stream().map(f -> tableManager.quote(f.getName())).collect(Collectors.joining(","));
        String placeholders = fields.stream().map(f -> "?").collect(Collectors.joining(","));
        String sql = "INSERT INTO " + tableManager.quote(config.destinationTable()) + " (" + columnList + ") VALUES (" + placeholders + ")";

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            int rowCount = root.getRowCount();
            for (int row = 0; row < rowCount; row++) {
                int paramIndex = 1;
                for (Field field : fields) {
                    FieldVector vector = root.getVector(field.getName());
                    bindParam(ps, paramIndex++, vector, row, field.getType());
                }
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private void bindParam(PreparedStatement ps, int index, FieldVector vector, int row, ArrowType type) throws SQLException {
        if (vector.isNull(row)) {
            ps.setObject(index, null);
            return;
        }
        if (type instanceof ArrowType.Int) {
            int bw = ((ArrowType.Int) type).getBitWidth();
            switch (bw) {
                case 8 -> ps.setByte(index, (byte) ((TinyIntVector) vector).get(row));
                case 16 -> ps.setShort(index, (short) ((SmallIntVector) vector).get(row));
                case 32 -> ps.setInt(index, ((IntVector) vector).get(row));
                case 64 -> ps.setLong(index, ((BigIntVector) vector).get(row));
                default -> throw new IllegalArgumentException("Unsupported int bit width: " + bw);
            }
            return;
        }
        if (type instanceof ArrowType.FloatingPoint fp) {
            switch (fp.getPrecision()) {
                case SINGLE -> ps.setFloat(index, ((Float4Vector) vector).get(row));
                case DOUBLE -> ps.setDouble(index, ((Float8Vector) vector).get(row));
                default -> throw new IllegalArgumentException("Unsupported floating precision: " + fp.getPrecision());
            }
            return;
        }
        if (type instanceof ArrowType.Bool) {
            ps.setBoolean(index, ((BitVector) vector).get(row) == 1);
            return;
        }
        if (type instanceof ArrowType.Utf8) {
            byte[] bytes = ((VarCharVector) vector).get(row);
            ps.setString(index, new String(bytes, StandardCharsets.UTF_8));
            return;
        }
        if (type instanceof ArrowType.Binary) {
            byte[] bytes = ((VarBinaryVector) vector).get(row);
            ps.setBytes(index, bytes);
            return;
        }
        if (type instanceof ArrowType.Struct || type instanceof ArrowType.List || type instanceof ArrowType.Map) {
            Object obj = vector.getObject(row);
            ps.setString(index, toJsonValue(obj));
            return;
        }
        throw new IllegalArgumentException("Unsupported Arrow type for insert: " + type);
    }

    private String toJsonValue(Object obj) {
        if (obj == null) return "null";
        if (obj instanceof CharSequence) {
            return '"' + escapeJson(obj.toString()) + '"';
        }
        if (obj instanceof Number || obj instanceof Boolean) {
            return obj.toString();
        }
        if (obj instanceof Map<?,?> map) {
            StringBuilder sb = new StringBuilder();
            sb.append('{');
            boolean first = true;
            for (var e : map.entrySet()) {
                if (!first) sb.append(',');
                first = false;
                sb.append('"').append(escapeJson(String.valueOf(e.getKey()))).append('"').append(':').append(toJsonValue(e.getValue()));
            }
            sb.append('}');
            return sb.toString();
        }
        if (obj instanceof Iterable<?> it) {
            StringBuilder sb = new StringBuilder();
            sb.append('[');
            boolean first = true;
            for (Object v : it) {
                if (!first) sb.append(',');
                first = false;
                sb.append(toJsonValue(v));
            }
            sb.append(']');
            return sb.toString();
        }
        if (obj.getClass().isArray()) {
            StringBuilder sb = new StringBuilder();
            sb.append('[');
            int len = java.lang.reflect.Array.getLength(obj);
            for (int i = 0; i < len; i++) {
                if (i > 0) sb.append(',');
                sb.append(toJsonValue(java.lang.reflect.Array.get(obj, i)));
            }
            sb.append(']');
            return sb.toString();
        }
        // Fallback to quoted string
        return '"' + escapeJson(obj.toString()) + '"';
    }

    private String escapeJson(String s) {
        StringBuilder sb = new StringBuilder();
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) sb.append(String.format("\\u%04x", (int)c)); else sb.append(c);
                }
            }
        }
        return sb.toString();
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
            LOG.log(System.Logger.Level.WARNING, "Failed to close writer connection: {0}", e.getMessage());
        }
    }
}
