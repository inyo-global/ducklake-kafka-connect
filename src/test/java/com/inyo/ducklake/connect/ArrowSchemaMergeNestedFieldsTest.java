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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.inyo.ducklake.ingestor.ArrowSchemaMerge;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/**
 * Test class to verify that identical schemas with nested address fields are correctly unified
 * without false "incompatible types" errors. This reproduces and validates the fix for the issue
 * where fields with the same name in different contexts were incorrectly reported as conflicting.
 */
public class ArrowSchemaMergeNestedFieldsTest {

  @Test
  public void testIdenticalSchemasWithNestedAddressFieldsUnifyCorrectly() {
    // Create schema based on your samples with both root address and client.address
    var addressStructFields =
        Arrays.asList(
            new Field("id", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
            new Field("stateCode", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
            new Field("countryCode", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
            new Field("city", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
            new Field("line1", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
            new Field("line2", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
            new Field("zipcode", FieldType.nullable(ArrowType.Utf8.INSTANCE), null));

    var personStructFields =
        Arrays.asList(
            new Field("id", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
            new Field("firstName", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
            new Field("lastName", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
            new Field("mainAddressId", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
            new Field("externalId", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
            new Field("updatedAt", FieldType.nullable(ArrowType.Utf8.INSTANCE), null));

    var clientStructFields =
        Arrays.asList(
            new Field("person", FieldType.nullable(ArrowType.Struct.INSTANCE), personStructFields),
            new Field(
                "address",
                FieldType.nullable(ArrowType.Struct.INSTANCE),
                addressStructFields) // client.address
            );

    // Schema 1 - representing first sample data structure
    var schema1Fields =
        Arrays.asList(
            new Field("id", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
            new Field("amount", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
            new Field("product", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
            new Field("client", FieldType.nullable(ArrowType.Struct.INSTANCE), clientStructFields),
            new Field(
                "address",
                FieldType.nullable(ArrowType.Struct.INSTANCE),
                addressStructFields) // root address
            );

    // Schema 2 - representing second sample data structure (identical to first)
    var schema2Fields =
        Arrays.asList(
            new Field("id", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
            new Field("amount", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
            new Field("product", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
            new Field("client", FieldType.nullable(ArrowType.Struct.INSTANCE), clientStructFields),
            new Field(
                "address",
                FieldType.nullable(ArrowType.Struct.INSTANCE),
                addressStructFields) // root address - IDENTICAL!
            );

    var schema1 = new Schema(schema1Fields);
    var schema2 = new Schema(schema2Fields);

    // Create sample values that would appear in the error message
    var sampleValues = new HashMap<String, List<Object>>();
    var structSample = new HashMap<String, Object>();
    structSample.put("id", "bc4ea138-bc4e-4d4f-aca4-4fb8d99bc4ea");
    structSample.put("zipcode", "81813");

    sampleValues.put("address", List.of(structSample));

    // This should work fine since both schemas are identical - no type conflicts
    var result = ArrowSchemaMerge.unifySchemas(Arrays.asList(schema1, schema2), () -> sampleValues);

    // Verify the unified schema works correctly
    assertEquals(5, result.getFields().size());

    // Find the address field in the result
    var addressField =
        result.getFields().stream()
            .filter(f -> f.getName().equals("address"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Address field not found"));

    // Verify that address field is correctly unified as Struct
    assertInstanceOf(ArrowType.Struct.class, addressField.getFieldType().getType());
    assertTrue(addressField.isNullable());
  }

  @Test
  public void testComplexNestedStructureUnifiesCorrectly() {
    // Test with the exact structure from your first sample
    var addressStructFields =
        Arrays.asList(
            new Field("id", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
            new Field("stateCode", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
            new Field("countryCode", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
            new Field("city", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
            new Field("line1", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
            new Field("line2", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
            new Field("zipcode", FieldType.nullable(ArrowType.Utf8.INSTANCE), null));

    var personStructFields =
        Arrays.asList(
            new Field("id", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
            new Field("firstName", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
            new Field("lastName", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
            new Field("mainAddressId", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
            new Field("externalId", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
            new Field("updatedAt", FieldType.nullable(ArrowType.Utf8.INSTANCE), null));

    var complianceLimitFields =
        Arrays.asList(
            new Field("amount", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
            new Field("currency", FieldType.nullable(ArrowType.Utf8.INSTANCE), null));

    var limitsFields =
        Arrays.asList(
            new Field(
                "oneDayLimit",
                FieldType.nullable(ArrowType.Struct.INSTANCE),
                complianceLimitFields),
            new Field(
                "thirtyDaysLimit",
                FieldType.nullable(ArrowType.Struct.INSTANCE),
                complianceLimitFields),
            new Field(
                "oneHundredAndEightyDaysLimit",
                FieldType.nullable(ArrowType.Struct.INSTANCE),
                complianceLimitFields));

    var currentComplianceLevelFields =
        Arrays.asList(
            new Field("level", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
            new Field("limits", FieldType.nullable(ArrowType.Struct.INSTANCE), limitsFields));

    var complianceLevelFields =
        List.of(
            new Field(
                "currentComplianceLevel",
                FieldType.nullable(ArrowType.Struct.INSTANCE),
                currentComplianceLevelFields));

    var schema1Fields =
        Arrays.asList(
            new Field(
                "address", FieldType.nullable(ArrowType.Struct.INSTANCE), addressStructFields),
            new Field("person", FieldType.nullable(ArrowType.Struct.INSTANCE), personStructFields),
            new Field(
                "complianceLevel",
                FieldType.nullable(ArrowType.Struct.INSTANCE),
                complianceLevelFields));

    var schema1 = new Schema(schema1Fields);

    // This should work fine for a single schema
    var result = ArrowSchemaMerge.unifySchemas(List.of(schema1), HashMap::new);
    assertEquals(3, result.getFields().size());
  }
}
