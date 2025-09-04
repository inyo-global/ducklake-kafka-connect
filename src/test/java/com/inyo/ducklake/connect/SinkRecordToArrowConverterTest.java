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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SinkRecordToArrowConverterTest {

  // Constants to avoid magic numbers
  private static final int EXPECTED_ROW_COUNT = 1;
  private static final int EXPECTED_TAG_COUNT = 3;
  private static final int EXPECTED_MAP_ENTRIES = 2;
  private static final int TEST_ID_VALUE = 42;
  private static final int TEST_ZIP_CODE = 12345;
  private static final long TEST_LONG_ID = 7L;
  private static final int FIRST_ELEMENT_INDEX = 0;
  private static final int SECOND_ELEMENT_INDEX = 1;
  private static final int THIRD_ELEMENT_INDEX = 2;
  private static final int MAP_VALUE_A = 1;
  private static final int MAP_VALUE_B = 2;

  @Test
  @DisplayName("Converts primitive STRUCT fields")
  void testPrimitiveStruct() {
    // Given
    /*
     * Kafka Schema JSON representation:
     * {
     *   "type": "STRUCT",
     *   "fields": [
     *     {"name": "id", "type": "INT32", "optional": false},
     *     {"name": "name", "type": "STRING", "optional": false}
     *   ]
     * }
     */
    var kafkaSchema =
        SchemaBuilder.struct()
            .field("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
            .field("name", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
            .build();
    var struct = new Struct(kafkaSchema).put("id", TEST_ID_VALUE).put("name", "alice");

    // When
    SinkRecord rec = new SinkRecord("topic", 0, null, null, kafkaSchema, struct, 0);
    var converter = new SinkRecordToArrowConverter(new RootAllocator());
    var root = converter.convertRecords(List.of(rec));

    // Then
    /*
     * Expected Arrow Schema JSON representation:
     * {
     *   "fields": [
     *     {"name": "id", "type": {"name": "int", "bitWidth": 32, "isSigned": true}, "nullable": false},
     *     {"name": "name", "type": {"name": "utf8"}, "nullable": false}
     *   ]
     * }
     */
    assertEquals(EXPECTED_ROW_COUNT, root.getRowCount());
    IntVector idVec = (IntVector) root.getVector("id");
    VarCharVector nameVec = (VarCharVector) root.getVector("name");
    assertEquals(TEST_ID_VALUE, idVec.get(FIRST_ELEMENT_INDEX));
    assertEquals("alice", new String(nameVec.get(FIRST_ELEMENT_INDEX), StandardCharsets.UTF_8));
    root.close();
  }

  @Test
  @DisplayName("Converts ARRAY field")
  void testArrayField() {
    // Given
    /*
     * Kafka Schema JSON representation:
     * {
     *   "type": "STRUCT",
     *   "fields": [
     *     {
     *       "name": "tags",
     *       "type": "ARRAY",
     *       "valueType": "STRING",
     *       "optional": false
     *     }
     *   ]
     * }
     */
    var kafkaSchema =
        SchemaBuilder.struct()
            .field(
                "tags",
                SchemaBuilder.array(org.apache.kafka.connect.data.Schema.STRING_SCHEMA).build())
            .build();
    var struct = new Struct(kafkaSchema).put("tags", List.of("red", "green", "blue"));

    // When
    SinkRecord rec = new SinkRecord("topic", 0, null, null, kafkaSchema, struct, 0);
    var converter = new SinkRecordToArrowConverter(new RootAllocator());
    var root = converter.convertRecords(List.of(rec));

    // Then
    /*
     * Expected Arrow Schema JSON representation:
     * {
     *   "fields": [
     *     {
     *       "name": "tags",
     *       "type": {"name": "list"},
     *       "nullable": false,
     *       "children": [
     *         {"name": "element", "type": {"name": "utf8"}, "nullable": false}
     *       ]
     *     }
     *   ]
     * }
     */
    ListVector tags = (ListVector) root.getVector("tags");
    assertEquals(EXPECTED_ROW_COUNT, root.getRowCount());
    assertEquals(EXPECTED_TAG_COUNT, tags.getInnerValueCountAt(FIRST_ELEMENT_INDEX));
    VarCharVector data = (VarCharVector) tags.getDataVector();
    assertEquals("red", new String(data.get(FIRST_ELEMENT_INDEX), StandardCharsets.UTF_8));
    assertEquals("green", new String(data.get(SECOND_ELEMENT_INDEX), StandardCharsets.UTF_8));
    assertEquals("blue", new String(data.get(THIRD_ELEMENT_INDEX), StandardCharsets.UTF_8));
    root.close();
  }

  @Test
  @DisplayName("Converts MAP field")
  void testMapField() {
    // Given
    /*
     * Kafka Schema JSON representation:
     * {
     *   "type": "STRUCT",
     *   "fields": [
     *     {
     *       "name": "meta",
     *       "type": "MAP",
     *       "keyType": "STRING",
     *       "valueType": "INT32",
     *       "optional": false
     *     }
     *   ]
     * }
     */
    var kafkaSchema =
        SchemaBuilder.struct()
            .field(
                "meta",
                SchemaBuilder.map(
                        org.apache.kafka.connect.data.Schema.STRING_SCHEMA,
                        org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
                    .build())
            .build();
    Map<String, Integer> map = new LinkedHashMap<>();
    map.put("a", MAP_VALUE_A);
    map.put("b", MAP_VALUE_B);
    var struct = new Struct(kafkaSchema).put("meta", map);

    // When
    SinkRecord rec = new SinkRecord("topic", 0, null, null, kafkaSchema, struct, 0);
    var converter = new SinkRecordToArrowConverter(new RootAllocator());
    var root = converter.convertRecords(List.of(rec));

    // Then
    /*
     * Expected Arrow Schema JSON representation:
     * {
     *   "fields": [
     *     {
     *       "name": "meta",
     *       "type": {"name": "map", "keysSorted": false},
     *       "nullable": false,
     *       "children": [
     *         {
     *           "name": "entries",
     *           "type": {"name": "struct"},
     *           "nullable": false,
     *           "children": [
     *             {"name": "key", "type": {"name": "utf8"}, "nullable": false},
     *             {"name": "value", "type": {"name": "int", "bitWidth": 32, "isSigned": true}, "nullable": false}
     *           ]
     *         }
     *       ]
     *     }
     *   ]
     * }
     */
    MapVector mv = (MapVector) root.getVector("meta");
    assertEquals(EXPECTED_ROW_COUNT, root.getRowCount());
    StructVector entries = (StructVector) mv.getDataVector();
    assertEquals(EXPECTED_MAP_ENTRIES, entries.getValueCount());
    VarCharVector keyVec = (VarCharVector) entries.getChild("key");
    IntVector valVec = (IntVector) entries.getChild("value");
    assertEquals("a", new String(keyVec.get(FIRST_ELEMENT_INDEX), StandardCharsets.UTF_8));
    assertEquals(MAP_VALUE_A, valVec.get(FIRST_ELEMENT_INDEX));
    assertEquals("b", new String(keyVec.get(SECOND_ELEMENT_INDEX), StandardCharsets.UTF_8));
    assertEquals(MAP_VALUE_B, valVec.get(SECOND_ELEMENT_INDEX));
    root.close();
  }

  @Test
  @DisplayName("Converts nested STRUCT")
  void testNestedStruct() {
    // Given
    /*
     * Inner Address Schema JSON representation:
     * {
     *   "type": "STRUCT",
     *   "fields": [
     *     {"name": "street", "type": "STRING", "optional": false},
     *     {"name": "zip", "type": "INT32", "optional": false}
     *   ]
     * }
     */
    var inner =
        SchemaBuilder.struct()
            .field("street", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
            .field("zip", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
            .build();

    /*
     * Outer Schema JSON representation:
     * {
     *   "type": "STRUCT",
     *   "fields": [
     *     {"name": "id", "type": "INT64", "optional": false},
     *     {"name": "address", "type": "STRUCT", "fields": [...inner], "optional": false}
     *   ]
     * }
     */
    var outer =
        SchemaBuilder.struct()
            .field("id", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
            .field("address", inner)
            .build();

    var innerStruct = new Struct(inner).put("street", "Main").put("zip", TEST_ZIP_CODE);
    var outerStruct = new Struct(outer).put("id", TEST_LONG_ID).put("address", innerStruct);

    // When
    SinkRecord rec = new SinkRecord("topic", 0, null, null, outer, outerStruct, 0);
    var converter = new SinkRecordToArrowConverter(new RootAllocator());
    var root = converter.convertRecords(List.of(rec));

    // Then
    /*
     * Expected Arrow Schema JSON representation:
     * {
     *   "fields": [
     *     {"name": "id", "type": {"name": "int", "bitWidth": 64, "isSigned": true}, "nullable": false},
     *     {
     *       "name": "address",
     *       "type": {"name": "struct"},
     *       "nullable": false,
     *       "children": [
     *         {"name": "street", "type": {"name": "utf8"}, "nullable": false},
     *         {"name": "zip", "type": {"name": "int", "bitWidth": 32, "isSigned": true}, "nullable": false}
     *       ]
     *     }
     *   ]
     * }
     */
    BigIntVector idVec = (BigIntVector) root.getVector("id");
    StructVector addressVec = (StructVector) root.getVector("address");
    VarCharVector street = (VarCharVector) addressVec.getChild("street");
    IntVector zip = (IntVector) addressVec.getChild("zip");
    assertEquals(TEST_LONG_ID, idVec.get(FIRST_ELEMENT_INDEX));
    assertEquals("Main", new String(street.get(FIRST_ELEMENT_INDEX), StandardCharsets.UTF_8));
    assertEquals(TEST_ZIP_CODE, zip.get(FIRST_ELEMENT_INDEX));
    root.close();
  }
}
