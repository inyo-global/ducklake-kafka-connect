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
package com.inyo.ducklake;

import java.sql.DriverManager;
import java.util.UUID;
import org.duckdb.DuckDBConnection;

public class TestHelper {

  public static DuckDBConnection setupDucklakeConnection() throws Exception {
    DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
    try (var st = conn.createStatement()) {
      st.execute(
          String.format(
              "ATTACH 'ducklake:%s' AS lake",
              System.getProperty("java.io.tmpdir") + "/" + UUID.randomUUID()));
    }
    return conn;
  }
}
