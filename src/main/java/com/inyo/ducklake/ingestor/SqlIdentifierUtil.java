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
package com.inyo.ducklake.ingestor;

/**
 * Utility for quoting of SQL identifiers (table and column names). DuckDB accepts various
 * identifier formats when properly quoted.
 */
public final class SqlIdentifierUtil {

  private SqlIdentifierUtil() {}

  /**
   * Quotes identifiers that contain characters outside the simple pattern.
   *
   * @param identifier identifier
   * @return identifier possibly within double quotes, with internal escaping
   */
  public static String quote(String identifier) {
    if (identifier == null) {
      throw new IllegalArgumentException("Identifier cannot be null");
    }
    return identifier.matches("[a-zA-Z_][a-zA-Z0-9_]*")
        ? identifier
        : '"' + identifier.replace("\"", "\"\"") + '"';
  }
}
