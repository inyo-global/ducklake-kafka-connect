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
    // Create connection with only non-extension-dependent properties.
    // S3 settings require the httpfs extension, so they must be set via SQL
    // after the connection is established and the extension is loaded.
    final Properties properties = new Properties();
    int threadCount = config.getDuckDbThreads();
    properties.setProperty("threads", String.valueOf(threadCount));
    this.conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:", properties);

    try (var st = conn.createStatement()) {
      // Only install/load httpfs and configure S3 when using S3 storage.
      // This avoids the "httpfs extension needs to be loaded" error that occurs
      // when S3 settings are passed as connection properties before the extension
      // can be autoloaded, while still allowing local file storage to work without
      // the httpfs extension.
      if (requiresS3Configuration()) {
        st.execute("INSTALL httpfs");
        st.execute("LOAD httpfs");
        st.execute("SET s3_url_style = '" + config.getS3UrlStyle() + "'");
        st.execute("SET s3_use_ssl = " + config.getS3UseSsl());
        st.execute("SET s3_endpoint = '" + config.getS3Endpoint() + "'");
        st.execute("SET s3_access_key_id = '" + config.getS3AccessKeyId() + "'");
        st.execute("SET s3_secret_access_key = '" + config.getS3SecretAccessKey() + "'");
      }

      // Attach the DuckLake catalog
      final String statement = buildAttachStatement();
      st.execute(statement);

      // Configure DuckLake retry count for handling PostgreSQL serialization conflicts
      final var maxRetryCount = config.getDucklakeMaxRetryCount();
      st.execute("SET ducklake_max_retry_count = " + maxRetryCount);
    }
  }

  /** Returns true if the data path uses S3 storage and requires httpfs configuration. */
  private boolean requiresS3Configuration() {
    String dataPath = config.getDataPath();
    return dataPath != null && (dataPath.startsWith("s3://") || dataPath.startsWith("s3a://"));
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
