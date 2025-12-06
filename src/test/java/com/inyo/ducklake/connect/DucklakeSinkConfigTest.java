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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

public class DucklakeSinkConfigTest {

  private DucklakeSinkConfig makeConfig(Map<String, String> props) {
    // Ensure required entries exist with sane defaults for tests
    if (!props.containsKey(DucklakeSinkConfig.DUCKLAKE_CATALOG_URI)) {
      props.put(DucklakeSinkConfig.DUCKLAKE_CATALOG_URI, "testcatalog");
    }
    if (!props.containsKey(DucklakeSinkConfig.DATA_PATH)) {
      props.put(DucklakeSinkConfig.DATA_PATH, "file:///tmp/data");
    }
    if (!props.containsKey(DucklakeSinkConfig.S3_URL_STYLE)) {
      props.put(DucklakeSinkConfig.S3_URL_STYLE, "path");
    }
    if (!props.containsKey(DucklakeSinkConfig.S3_USE_SSL)) {
      props.put(DucklakeSinkConfig.S3_USE_SSL, "true");
    }
    if (!props.containsKey(DucklakeSinkConfig.S3_ENDPOINT)) {
      props.put(DucklakeSinkConfig.S3_ENDPOINT, "https://example.com");
    }
    if (!props.containsKey(DucklakeSinkConfig.S3_ACCESS_KEY_ID)) {
      props.put(DucklakeSinkConfig.S3_ACCESS_KEY_ID, "key");
    }
    if (!props.containsKey(DucklakeSinkConfig.S3_SECRET_ACCESS_KEY)) {
      props.put(DucklakeSinkConfig.S3_SECRET_ACCESS_KEY, "secret");
    }
    var def = DucklakeSinkConfig.CONFIG_DEF;
    return new DucklakeSinkConfig(def, props);
  }

  @Test
  public void parse_defaultIs10000() {
    var props = new HashMap<String, String>();
    var cfg = makeConfig(props);
    var val = cfg.getDataInliningRowLimit();
    // Default is 'off' -> disabled
    assertFalse(val.isPresent());
  }

  @Test
  public void parse_numericValueReturnsNumber() {
    var props = new HashMap<String, String>();
    props.put(DucklakeSinkConfig.DATA_INLINING_ROW_LIMIT, "5000");
    var cfg = makeConfig(props);
    var val = cfg.getDataInliningRowLimit();
    assertTrue(val.isPresent());
    assertEquals(5000, val.getAsInt());
  }

  @Test
  public void parse_offDisablesFeature() {
    var props = new HashMap<String, String>();
    props.put(DucklakeSinkConfig.DATA_INLINING_ROW_LIMIT, "off");
    var cfg = makeConfig(props);
    var val = cfg.getDataInliningRowLimit();
    assertFalse(val.isPresent());
  }

  @Test
  public void parse_invalidValueThrows() {
    var props = new HashMap<String, String>();
    props.put(DucklakeSinkConfig.DATA_INLINING_ROW_LIMIT, "notanumber");
    assertThrows(ConfigException.class, () -> makeConfig(props).getDataInliningRowLimit());
  }
}
