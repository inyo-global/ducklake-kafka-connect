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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ArrowIpcConverterSimpleTest {

  private ArrowIpcConverter converter;

  @BeforeEach
  void setUp() {
    converter = new ArrowIpcConverter();
    Map<String, Object> configs = new HashMap<>();
    converter.configure(configs, false);
  }

  @AfterEach
  void tearDown() {
    converter.close();
  }

  @Test
  void shouldHandleNullValueInSerialization() {
    byte[] result = converter.fromConnectData("test-topic", null, null);
    assertNull(result);
  }

  @Test
  void shouldThrowDataExceptionForInvalidObjectType() {
    String invalidValue = "not a VectorSchemaRoot";

    assertThrows(
        DataException.class, () -> converter.fromConnectData("test-topic", null, invalidValue));
  }

  @Test
  void shouldHandleNullBytesInDeserialization() {
    var result = converter.toConnectData("test-topic", null);

    assertNotNull(result);
    assertNull(result.schema());
    assertNull(result.value());
  }

  @Test
  void shouldHandleEmptyBytesInDeserialization() {
    var result = converter.toConnectData("test-topic", new byte[0]);

    assertNotNull(result);
    assertNull(result.schema());
    assertNull(result.value());
  }

  @Test
  void shouldConfigureCorrectly() {
    var testConverter = new ArrowIpcConverter();
    Map<String, Object> configs = new HashMap<>();

    assertDoesNotThrow(() -> testConverter.configure(configs, true));

    testConverter.close();
  }

  @Test
  void shouldCloseResourcesGracefully() {
    var testConverter = new ArrowIpcConverter();
    Map<String, Object> configs = new HashMap<>();
    testConverter.configure(configs, false);

    assertDoesNotThrow(
        () -> {
          testConverter.close();
          testConverter.close(); // Multiple calls should not fail
        });
  }
}
