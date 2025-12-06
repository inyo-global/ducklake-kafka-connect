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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class DucklakeConnectionFactoryIntegrationTest {

  private DucklakeSinkConfig makeConfig(Map<String, String> props) {
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
  public void attachStatement_includesRowLimitWhenEnabled() {
    var props = new HashMap<String, String>();
    props.put(DucklakeSinkConfig.DUCKLAKE_CATALOG_URI, "inlining.duckdb");
    props.put(DucklakeSinkConfig.DATA_PATH, "file:///tmp/data");
    props.put(DucklakeSinkConfig.DATA_INLINING_ROW_LIMIT, "10");
    var cfg = makeConfig(props);
    var factory = new DucklakeConnectionFactory(cfg);
    var statement = factory.buildAttachStatement();
    assertTrue(statement.contains("DATA_INLINING_ROW_LIMIT 10"));
  }

  @Test
  public void attachStatement_omitsRowLimitWhenOff() {
    var props = new HashMap<String, String>();
    props.put(DucklakeSinkConfig.DUCKLAKE_CATALOG_URI, "inlining.duckdb");
    props.put(DucklakeSinkConfig.DATA_PATH, "file:///tmp/data");
    props.put(DucklakeSinkConfig.DATA_INLINING_ROW_LIMIT, "off");
    var cfg = makeConfig(props);
    var factory = new DucklakeConnectionFactory(cfg);
    var statement = factory.buildAttachStatement();
    assertFalse(statement.contains("DATA_INLINING_ROW_LIMIT"));
  }
}
