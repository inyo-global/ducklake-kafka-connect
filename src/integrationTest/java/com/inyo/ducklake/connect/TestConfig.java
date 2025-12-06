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

import java.util.HashMap;
import org.jetbrains.annotations.NotNull;

public final class TestConfig {
  private TestConfig() {}

  public static @NotNull HashMap<String, String> getBaseDucklakeConfig() {
    var base = new HashMap<String, String>();
    base.put(DucklakeSinkConfig.DUCKLAKE_CATALOG_URI, "memory");
    base.put("ducklake.data_path", "file:///tmp/ducklake-test/");
    base.put("s3.url_style", "path");
    base.put("s3.endpoint", "localhost:9000");
    base.put("s3.access_key_id", "minio");
    base.put("s3.secret_access_key", "minio123");
    return base;
  }
}
