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

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.duckdb.DuckDBConnection;

public class DucklakeConnectionFactory {

  private final DucklakeSinkConfig config;
  private DuckDBConnection conn;

  public DucklakeConnectionFactory(DucklakeSinkConfig config) {
    this.config = config;
  }

  @SuppressFBWarnings(
      value = "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE",
      justification = "PreparedStatement cannot parameterize DDL identifiers.")
  public void create() throws SQLException {
    if (this.conn != null) {
      return;
    }
    this.conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:", getProperties());
    var statement = buildAttachStatement();
    try (var st = conn.createStatement()) {
      // Load/install the httpfs extension (moved to helper for clarity).
      preloadHttpfsExtension(st);

      // After httpfs is loaded, configure S3/httpfs-related settings via SET statements.
      final var s3UrlStyle = config.getS3UrlStyle();
      if (s3UrlStyle != null && !s3UrlStyle.isBlank()) {
        st.execute("SET s3_url_style = '" + s3UrlStyle.replace("'", "''") + "'");
      }
      final var s3UseSsl = config.getS3UseSsl();
      if (s3UseSsl != null && !s3UseSsl.isBlank()) {
        st.execute("SET s3_use_ssl = '" + s3UseSsl.replace("'", "''") + "'");
      }
      final var s3Endpoint = config.getS3Endpoint();
      if (s3Endpoint != null && !s3Endpoint.isBlank()) {
        st.execute("SET s3_endpoint = '" + s3Endpoint.replace("'", "''") + "'");
      }
      final var s3AccessKey = config.getS3AccessKeyId();
      if (s3AccessKey != null && !s3AccessKey.isBlank()) {
        st.execute("SET s3_access_key_id = '" + s3AccessKey.replace("'", "''") + "'");
      }
      final var s3Secret = config.getS3SecretAccessKey();
      if (s3Secret != null && !s3Secret.isBlank()) {
        st.execute("SET s3_secret_access_key = '" + s3Secret.replace("'", "''") + "'");
      }

      st.execute(statement);
      // Configure DuckLake retry count for handling PostgreSQL serialization conflicts
      final var maxRetryCount = config.getDucklakeMaxRetryCount();
      st.execute("SET ducklake_max_retry_count = " + maxRetryCount);
    }
  }

  private void preloadHttpfsExtension(Statement st) throws SQLException {
    // Prefer loading the extension if it's already installed. If LOAD fails because the
    // extension is missing, attempt INSTALL then LOAD. Surface a helpful error if both fail.
    try {
      st.execute("LOAD httpfs;");
    } catch (SQLException loadEx) {
      try {
        st.execute("INSTALL httpfs;");
        st.execute("LOAD httpfs;");
      } catch (SQLException installEx) {
        var msg = "Failed to load or install the DuckDB 'httpfs' extension." +
            " Ensure the environment allows DuckDB to download and write extensions" +
            " or install the extension manually (e.g. run 'INSTALL httpfs' in a DuckDB shell). " +
            "Load error: " + loadEx.getMessage() +
            "; install error: " + installEx.getMessage();
        throw new SQLException(msg, installEx);
      }
    }
  }

  @NonNull
  private Properties getProperties() {
    var properties = new Properties();
    // Only pass properties that do not require httpfs at startup (threads only).
    var threadCount = config.getDuckDbThreads();
    properties.setProperty("threads", String.valueOf(threadCount));
    return properties;
  }

  /* package */ String buildAttachStatement() {
    var sb = new StringBuilder();
    sb.append("ATTACH IF NOT EXISTS 'ducklake:");
    sb.append(config.getDucklakeCatalogUri());
    sb.append("' AS lake (");
    sb.append("DATA_PATH '");
    sb.append(config.getDataPath());
    sb.append("'");
    var maybeInline = config.getDataInliningRowLimit();
    if (maybeInline.isPresent()) {
      sb.append(", DATA_INLINING_ROW_LIMIT ");
      sb.append(maybeInline.getAsInt());
    }
    sb.append(");");
    return sb.toString();
  }

  public DuckDBConnection getConnection() {
    if (conn == null) {
      throw new IllegalStateException("Connection not initialized. Call create() first.");
    }
    try {
      return conn.duplicate();
    } catch (SQLException e) {
      throw new RuntimeException("Failed to duplicate DuckDB connection", e);
    }
  }

  public void close() {
    if (conn != null) {
      try {
        conn.close();
      } catch (SQLException e) {
        throw new RuntimeException("Failed to close DuckDB connection", e);
      } finally {
        conn = null;
      }
    }
  }
}
