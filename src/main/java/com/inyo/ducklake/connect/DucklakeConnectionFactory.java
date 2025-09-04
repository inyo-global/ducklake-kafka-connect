package com.inyo.ducklake.connect;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.DriverManager;
import java.sql.SQLException;
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
    final Properties properties = new Properties();
    properties.setProperty("s3_url_style", config.getS3UrlStyle());
    properties.setProperty("s3_use_ssl", config.getS3UseSsl());
    properties.setProperty("s3_endpoint", config.getS3Endpoint());
    properties.setProperty("s3_access_key_id", config.getS3AccessKeyId());
    properties.setProperty("s3_secret_access_key", config.getS3SecretAccessKey());
    properties.setProperty("threads", "1");
    this.conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:", properties);
    final String statement =
        String.format(
            "ATTACH IF NOT EXISTS 'ducklake:%s' AS lake (DATA_PATH '%s');",
            config.getDucklakeCatalogUri(), config.getDataPath());
    try (var st = conn.createStatement()) {
      st.execute(statement);
    }
  }

  public DuckDBConnection getConnection() {
    if (conn == null) {
      throw new IllegalStateException("Connection not initialized. Call create() first.");
    }
    try {
      return (DuckDBConnection) conn.duplicate();
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
