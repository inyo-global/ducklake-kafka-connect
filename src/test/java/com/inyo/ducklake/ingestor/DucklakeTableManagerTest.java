package com.inyo.ducklake.ingestor;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.*;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class DucklakeTableManagerTest {

    DuckDBConnection conn;

    @BeforeEach
    void setup() throws Exception {
        Connection c = DriverManager.getConnection("jdbc:duckdb:"); // in-memory
        conn = c.unwrap(DuckDBConnection.class);
    }

    @AfterEach
    void tearDown() throws Exception {
        conn.close();
    }

    private Schema schema(Field... fields) {
        return new Schema(Arrays.asList(fields));
    }

    private Field intField(String name, int bits) {
        return new Field(name, new FieldType(false, new ArrowType.Int(bits, true), null), null);
    }

    private Field floatField(String name, boolean doublePrec) {
        return new Field(name, new FieldType(false,
                new ArrowType.FloatingPoint(doublePrec ? FloatingPointPrecision.DOUBLE : FloatingPointPrecision.SINGLE), null), null);
    }

    private Field stringField(String name) {
        return new Field(name, new FieldType(false, ArrowType.Utf8.INSTANCE, null), null);
    }

    private Field binaryField(String name) {
        return new Field(name, new FieldType(false, ArrowType.Binary.INSTANCE, null), null);
    }

    private Field structField(String name, Field... children) {
        return new Field(name, new FieldType(false, ArrowType.Struct.INSTANCE, null), Arrays.asList(children));
    }

    private Field listField(String name, Field elementField) {
        return new Field(name, new FieldType(false, ArrowType.List.INSTANCE, null), List.of(elementField));
    }

    private Field mapField(String name, Field keyField, Field valueField) {
        return new Field(name, new FieldType(false, new ArrowType.Map(true), null), List.of(keyField, valueField));
    }

    @Test
    @DisplayName("Creates table when absent and autoCreate=true")
    void testCreateTableAuto() throws Exception {
        DucklakeWriterConfig cfg = new DucklakeWriterConfig("t1", true, new String[]{"id"}, new String[0]);
        DucklakeTableManager mgr = new DucklakeTableManager(conn, cfg);
        Schema s = schema(intField("id",32), stringField("name"));
        mgr.ensureTable(s);

        // Verify existence
        try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM information_schema.columns WHERE table_name='t1'")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
        }
    }

    @Test
    @DisplayName("Fails if table does not exist and autoCreate=false")
    void testCreateTableDenied() {
        DucklakeWriterConfig cfg = new DucklakeWriterConfig("t2", false, new String[]{}, new String[0]);
        DucklakeTableManager mgr = new DucklakeTableManager(conn, cfg);
        Schema s = schema(intField("a",32));
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> mgr.ensureTable(s));
        assertTrue(ex.getMessage().contains("auto-create"));
    }

    @Test
    @DisplayName("Adds new column during evolution")
    void testAddNewColumnEvolution() throws Exception {
        DucklakeWriterConfig cfg = new DucklakeWriterConfig("t3", true, new String[]{}, new String[0]);
        DucklakeTableManager mgr = new DucklakeTableManager(conn, cfg);
        mgr.ensureTable(schema(intField("a",32)));
        // Evolve adding new column
        mgr.ensureTable(schema(intField("a",32), stringField("b")));

        Set<String> cols = getColumns("t3");
        assertEquals(Set.of("a","b"), cols);
    }

    @Test
    @DisplayName("Accepts integer width promotion (existing wider)")
    void testIntegerPromotion() throws Exception {
        DucklakeWriterConfig cfg = new DucklakeWriterConfig("t4", true, new String[]{}, new String[0]);
        DucklakeTableManager mgr = new DucklakeTableManager(conn, cfg);
        // create with BIGINT
        mgr.ensureTable(schema(intField("num",64)));
        // new definition with INT32 should be accepted
        mgr.ensureTable(schema(intField("num",32)));
        // still only one column
        assertEquals(Set.of("num"), getColumns("t4"));
    }

    @Test
    @DisplayName("Rejects incompatible type")
    void testIncompatibleType() throws Exception {
        DucklakeWriterConfig cfg = new DucklakeWriterConfig("t5", true, new String[]{}, new String[0]);
        DucklakeTableManager mgr = new DucklakeTableManager(conn, cfg);
        // create with VARCHAR
        mgr.ensureTable(schema(stringField("c")));
        // attempt to evolve to INT -> should fail
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> mgr.ensureTable(schema(intField("c",32))));
        assertTrue(ex.getMessage().contains("Incompatible type"));
    }

    @Test
    @DisplayName("Does not add duplicate column")
    void testNoDuplicateAdd() throws Exception {
        DucklakeWriterConfig cfg = new DucklakeWriterConfig("t6", true, new String[]{}, new String[0]);
        DucklakeTableManager mgr = new DucklakeTableManager(conn, cfg);
        mgr.ensureTable(schema(intField("x",32), stringField("y")));
        // same definition again
        mgr.ensureTable(schema(intField("x",32), stringField("y")));
        assertEquals(Set.of("x","y"), getColumns("t6"));
    }

    @Test
    @DisplayName("Accepts expected FLOAT when existing is DOUBLE")
    void testFloatDoubleCompatibility() throws Exception {
        DucklakeWriterConfig cfg = new DucklakeWriterConfig("t7", true, new String[]{}, new String[0]);
        DucklakeTableManager mgr = new DucklakeTableManager(conn, cfg);
        mgr.ensureTable(schema(floatField("v", true))); // DOUBLE
        mgr.ensureTable(schema(floatField("v", false))); // expected FLOAT
        assertEquals(Set.of("v"), getColumns("t7"));
    }

    @Test
    @DisplayName("Creates JSON column for STRUCT field")
    void testStructCreatesJson() throws Exception {
        DucklakeWriterConfig cfg = new DucklakeWriterConfig("t_struct", true, new String[]{}, new String[0]);
        DucklakeTableManager mgr = new DucklakeTableManager(conn, cfg);
        Field child = stringField("name");
        Field struct = structField("payload", child);
        mgr.ensureTable(schema(struct));
        assertColumnType("t_struct", "payload", "JSON");
    }

    @Test
    @DisplayName("Creates JSON column for LIST field")
    void testListCreatesJson() throws Exception {
        DucklakeWriterConfig cfg = new DucklakeWriterConfig("t_list", true, new String[]{}, new String[0]);
        DucklakeTableManager mgr = new DucklakeTableManager(conn, cfg);
        Field element = stringField("element");
        Field list = listField("tags", element);
        mgr.ensureTable(schema(list));
        assertColumnType("t_list", "tags", "JSON");
    }

    @Test
    @DisplayName("Creates JSON column for MAP field")
    void testMapCreatesJson() throws Exception {
        DucklakeWriterConfig cfg = new DucklakeWriterConfig("t_map", true, new String[]{}, new String[0]);
        DucklakeTableManager mgr = new DucklakeTableManager(conn, cfg);
        Field key = stringField("key");
        Field value = stringField("value");
        Field map = mapField("attributes", key, value);
        mgr.ensureTable(schema(map));
        assertColumnType("t_map", "attributes", "JSON");
    }

    @Test
    @DisplayName("Evolves adding new JSON column from LIST")
    void testAddJsonColumnEvolution() throws Exception {
        DucklakeWriterConfig cfg = new DucklakeWriterConfig("t_json_evo", true, new String[]{}, new String[0]);
        DucklakeTableManager mgr = new DucklakeTableManager(conn, cfg);
        mgr.ensureTable(schema(stringField("id")));
        Field element = stringField("element");
        Field list = listField("items", element);
        mgr.ensureTable(schema(stringField("id"), list));
        assertEquals(Set.of("id","items"), getColumns("t_json_evo"));
        assertColumnType("t_json_evo", "items", "JSON");
    }

    @Test
    @DisplayName("Populates metadata cache after first ensureTable")
    void testMetadataCachePopulation() throws Exception {
        DucklakeWriterConfig cfg = new DucklakeWriterConfig("t_cache1", true, new String[]{}, new String[0]);
        DucklakeTableManager mgr = new DucklakeTableManager(conn, cfg);
        assertFalse(mgr.isMetadataCached());
        mgr.ensureTable(schema(intField("id",32), stringField("name")));
        assertTrue(mgr.isMetadataCached());
        assertEquals(Set.of("id","name"), mgr.cachedColumnNames());
        // Call again with same schema (should not change cache contents)
        mgr.ensureTable(schema(intField("id",32), stringField("name")));
        assertEquals(Set.of("id","name"), mgr.cachedColumnNames());
    }

    @Test
    @DisplayName("Updates metadata cache incrementally on evolution")
    void testMetadataCacheUpdatedOnEvolution() throws Exception {
        DucklakeWriterConfig cfg = new DucklakeWriterConfig("t_cache2", true, new String[]{}, new String[0]);
        DucklakeTableManager mgr = new DucklakeTableManager(conn, cfg);
        mgr.ensureTable(schema(intField("a",32)));
        assertEquals(Set.of("a"), mgr.cachedColumnNames());
        mgr.ensureTable(schema(intField("a",32), stringField("b")));
        assertEquals(Set.of("a","b"), mgr.cachedColumnNames());
    }

    @Test
    @DisplayName("Upgrades INTEGER to BIGINT on evolution")
    void testIntegerTypeUpgrade() throws Exception {
        DucklakeWriterConfig cfg = new DucklakeWriterConfig("t_upgrade_int", true, new String[]{}, new String[0]);
        DucklakeTableManager mgr = new DucklakeTableManager(conn, cfg);
        // create with INTEGER
        mgr.ensureTable(schema(intField("num",32)));
        assertColumnType("t_upgrade_int", "num", "INTEGER");
        // evolve to BIGINT
        mgr.ensureTable(schema(intField("num",64)));
        assertColumnType("t_upgrade_int", "num", "BIGINT");
    }

    @Test
    @DisplayName("Upgrades FLOAT to DOUBLE on evolution")
    void testFloatTypeUpgrade() throws Exception {
        DucklakeWriterConfig cfg = new DucklakeWriterConfig("t_upgrade_float", true, new String[]{}, new String[0]);
        DucklakeTableManager mgr = new DucklakeTableManager(conn, cfg);
        // create with FLOAT
        mgr.ensureTable(schema(floatField("v", false))); // FLOAT
        assertColumnType("t_upgrade_float", "v", "FLOAT");
        // evolve to DOUBLE
        mgr.ensureTable(schema(floatField("v", true))); // DOUBLE
        assertColumnType("t_upgrade_float", "v", "DOUBLE");
    }

    @Test
    @DisplayName("Does not downgrade BIGINT to INTEGER")
    void testNoIntegerDowngrade() throws Exception {
        DucklakeWriterConfig cfg = new DucklakeWriterConfig("t_no_down_int", true, new String[]{}, new String[0]);
        DucklakeTableManager mgr = new DucklakeTableManager(conn, cfg);
        mgr.ensureTable(schema(intField("num",64))); // BIGINT
        assertColumnType("t_no_down_int", "num", "BIGINT");
        // attempt to downgrade to INTEGER
        mgr.ensureTable(schema(intField("num",32))); // should keep BIGINT
        assertColumnType("t_no_down_int", "num", "BIGINT");
    }

    @Test
    @DisplayName("Does not downgrade DOUBLE to FLOAT")
    void testNoFloatDowngrade() throws Exception {
        DucklakeWriterConfig cfg = new DucklakeWriterConfig("t_no_down_float", true, new String[]{}, new String[0]);
        DucklakeTableManager mgr = new DucklakeTableManager(conn, cfg);
        mgr.ensureTable(schema(floatField("v", true))); // DOUBLE
        assertColumnType("t_no_down_float", "v", "DOUBLE");
        // attempt to downgrade to FLOAT
        mgr.ensureTable(schema(floatField("v", false))); // should keep DOUBLE
        assertColumnType("t_no_down_float", "v", "DOUBLE");
    }

    @Test
    @DisplayName("Rejects evolution from JSON (STRUCT) to VARCHAR")
    void testJsonToVarcharIncompatible() throws Exception {
        DucklakeWriterConfig cfg = new DucklakeWriterConfig("t_json_incompat", true, new String[]{}, new String[0]);
        DucklakeTableManager mgr = new DucklakeTableManager(conn, cfg);
        // initial STRUCT -> JSON
        Field struct = structField("payload", stringField("x"));
        mgr.ensureTable(schema(struct));
        assertColumnType("t_json_incompat", "payload", "JSON");
        // attempt to change to VARCHAR
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                mgr.ensureTable(schema(stringField("payload")))
        );
        assertTrue(ex.getMessage().contains("Incompatible type"));
    }

    private Set<String> getColumns(String table) throws Exception {
        Set<String> set = new LinkedHashSet<>();
        try (PreparedStatement ps = conn.prepareStatement("PRAGMA table_info("+table+")")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    set.add(rs.getString("name"));
                }
            }
        }
        return set;
    }

    private void assertColumnType(String table, String column, String expectedType) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("PRAGMA table_info("+table+")")) {
            try (ResultSet rs = ps.executeQuery()) {
                boolean found = false;
                while (rs.next()) {
                    if (rs.getString("name").equals(column)) {
                        found = true;
                        assertEquals(expectedType, rs.getString("type"));
                    }
                }
                assertTrue(found, "Column not found: " + column);
            }
        }
    }
}
