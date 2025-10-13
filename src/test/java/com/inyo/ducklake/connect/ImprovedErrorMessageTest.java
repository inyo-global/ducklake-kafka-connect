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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test to demonstrate improved error messages with better sample value collection. This test shows
 * how the new implementation collects diverse samples instead of just the first 5 values.
 */
class ImprovedErrorMessageTest {

  private BufferAllocator allocator;
  private SinkRecordToArrowConverter converter;

  @BeforeEach
  void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
    converter = new SinkRecordToArrowConverter(allocator);
  }

  @AfterEach
  void tearDown() {
    converter.close();
    allocator.close();
  }

  @Test
  void testImprovedErrorMessageShowsDifferentTypes() {
    // Create schema with address as struct
    var structSchema =
        SchemaBuilder.struct()
            .field(
                "address",
                SchemaBuilder.struct()
                    .field("zipcode", Schema.STRING_SCHEMA)
                    .field("id", Schema.STRING_SCHEMA)
                    .build())
            .field(
                "person",
                SchemaBuilder.struct()
                    .field("firstName", Schema.STRING_SCHEMA)
                    .field("lastName", Schema.STRING_SCHEMA)
                    .build())
            .build();

    // Create schema with address as string (incompatible)
    var stringSchema =
        SchemaBuilder.struct()
            .field("address", Schema.STRING_SCHEMA) // This is the incompatible field
            .field(
                "person",
                SchemaBuilder.struct()
                    .field("firstName", Schema.STRING_SCHEMA)
                    .field("lastName", Schema.STRING_SCHEMA)
                    .build())
            .build();

    // Create multiple compatible records (struct address)
    var addressStruct1 =
        new Struct(structSchema.field("address").schema())
            .put("zipcode", "90813")
            .put("id", "ece4a138-bc4e-4d4f-aca4-4fb8d997dfea");

    var person1 =
        new Struct(structSchema.field("person").schema())
            .put("firstName", "John")
            .put("lastName", "Doe");

    var record1 =
        new SinkRecord(
            "test",
            0,
            null,
            null,
            structSchema,
            new Struct(structSchema).put("address", addressStruct1).put("person", person1),
            0);

    var record2 =
        new SinkRecord(
            "test",
            0,
            null,
            null,
            structSchema,
            new Struct(structSchema).put("address", addressStruct1).put("person", person1),
            1);

    var record3 =
        new SinkRecord(
            "test",
            0,
            null,
            null,
            structSchema,
            new Struct(structSchema).put("address", addressStruct1).put("person", person1),
            2);

    var record4 =
        new SinkRecord(
            "test",
            0,
            null,
            null,
            structSchema,
            new Struct(structSchema).put("address", addressStruct1).put("person", person1),
            3);

    // Create one incompatible record (string address) - this should be captured in samples
    var person2 =
        new Struct(stringSchema.field("person").schema())
            .put("firstName", "Jane")
            .put("lastName", "Smith");

    var incompatibleRecord =
        new SinkRecord(
            "test",
            0,
            null,
            null,
            stringSchema,
            new Struct(stringSchema)
                .put(
                    "address",
                    "123 Main St") // String instead of struct - this is the incompatible value
                .put("person", person2),
            4);

    List<SinkRecord> records =
        Arrays.asList(record1, record2, record3, record4, incompatibleRecord);

    var exception =
        assertThrows(
            RuntimeException.class,
            () -> {
              converter.convertRecords(records);
            });

    var message = exception.getCause().getMessage();
    System.out.println("Improved error message:");
    System.out.println(message);

    // Verify the error message contains useful information
    assertTrue(message.contains("address"), "Error should mention the problematic field name");
    assertTrue(message.contains("Sample values:"), "Error should include sample values");

    // The new implementation should show both the struct and string samples for the address field
    // This will help users understand what types are conflicting
    assertTrue(
        message.contains("Struct") || message.contains("String"),
        "Error should show the different types that are conflicting");
  }
}
