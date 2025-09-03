package com.inyo.ducklake.connect;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class SinkRecordToArrowConverterTest {

    @Test
    @DisplayName("Converts primitive STRUCT fields")
    void testPrimitiveStruct() {
        var kafkaSchema = SchemaBuilder.struct()
                .field("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
                .field("name", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                .build();
        var struct = new Struct(kafkaSchema)
                .put("id", 42)
                .put("name", "alice");

        SinkRecord rec = new SinkRecord("topic", 0, null, null, kafkaSchema, struct, 0);
        var converter = new SinkRecordToArrowConverter(new RootAllocator());
        var root = converter.convertRecords(List.of(rec));
        assertEquals(1, root.getRowCount());
        IntVector idVec = (IntVector) root.getVector("id");
        VarCharVector nameVec = (VarCharVector) root.getVector("name");
        assertEquals(42, idVec.get(0));
        assertEquals("alice", new String(nameVec.get(0)));
        root.close();
    }

    @Test
    @DisplayName("Converts ARRAY field")
    void testArrayField() {
        var kafkaSchema = SchemaBuilder.struct()
                .field("tags", SchemaBuilder.array(org.apache.kafka.connect.data.Schema.STRING_SCHEMA).build())
                .build();
        var struct = new Struct(kafkaSchema)
                .put("tags", List.of("red", "green", "blue"));
        SinkRecord rec = new SinkRecord("topic", 0, null, null, kafkaSchema, struct, 0);
        var converter = new SinkRecordToArrowConverter(new RootAllocator());
        var root = converter.convertRecords(List.of(rec));
        ListVector tags = (ListVector) root.getVector("tags");
        assertEquals(1, root.getRowCount());
        assertEquals(3, tags.getInnerValueCountAt(0));
        VarCharVector data = (VarCharVector) tags.getDataVector();
        assertEquals("red", new String(data.get(0)));
        assertEquals("green", new String(data.get(1)));
        assertEquals("blue", new String(data.get(2)));
        root.close();
    }

    @Test
    @DisplayName("Converts MAP field")
    void testMapField() {
        var kafkaSchema = SchemaBuilder.struct()
                .field("meta", SchemaBuilder.map(org.apache.kafka.connect.data.Schema.STRING_SCHEMA, org.apache.kafka.connect.data.Schema.INT32_SCHEMA).build())
                .build();
        Map<String, Integer> map = new LinkedHashMap<>();
        map.put("a", 1); map.put("b", 2);
        var struct = new Struct(kafkaSchema).put("meta", map);
        SinkRecord rec = new SinkRecord("topic", 0, null, null, kafkaSchema, struct, 0);
        var converter = new SinkRecordToArrowConverter(new RootAllocator());
        var root = converter.convertRecords(List.of(rec));
        MapVector mv = (MapVector) root.getVector("meta");
        assertEquals(1, root.getRowCount());
        StructVector entries = (StructVector) mv.getDataVector();
        assertEquals(2, entries.getValueCount());
        VarCharVector keyVec = (VarCharVector) entries.getChild("key");
        IntVector valVec = (IntVector) entries.getChild("value");
        assertEquals("a", new String(keyVec.get(0)));
        assertEquals(1, valVec.get(0));
        assertEquals("b", new String(keyVec.get(1)));
        assertEquals(2, valVec.get(1));
        root.close();
    }

    @Test
    @DisplayName("Converts nested STRUCT")
    void testNestedStruct() {
        var inner = SchemaBuilder.struct()
                .field("street", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                .field("zip", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
                .build();
        var outer = SchemaBuilder.struct()
                .field("id", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
                .field("address", inner)
                .build();
        var innerStruct = new Struct(inner).put("street", "Main").put("zip", 12345);
        var outerStruct = new Struct(outer).put("id", 7L).put("address", innerStruct);
        SinkRecord rec = new SinkRecord("topic", 0, null, null, outer, outerStruct, 0);
        var converter = new SinkRecordToArrowConverter(new RootAllocator());
        var root = converter.convertRecords(List.of(rec));
        BigIntVector idVec = (BigIntVector) root.getVector("id");
        StructVector addressVec = (StructVector) root.getVector("address");
        VarCharVector street = (VarCharVector) addressVec.getChild("street");
        IntVector zip = (IntVector) addressVec.getChild("zip");
        assertEquals(7L, idVec.get(0));
        assertEquals("Main", new String(street.get(0)));
        assertEquals(12345, zip.get(0));
        root.close();
    }
}

