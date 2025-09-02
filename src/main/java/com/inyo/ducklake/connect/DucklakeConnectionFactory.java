package com.inyo.ducklake.connect;

import org.duckdb.DuckDBConnection;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DucklakeConnectionFactory {

    private final DucklakeSinkConfig config;
    private DuckDBConnection conn;

    public DucklakeConnectionFactory(DucklakeSinkConfig config) {
        this.config = config;
    }

    public void create() throws SQLException {
        final Properties properties = new Properties();
        properties.setProperty("s3_url_style", config.getS3UrlStyle());
        properties.setProperty("s3_use_ssl", config.getS3UseSsl());
        properties.setProperty("s3_endpoint", config.getS3Endpoint());
        properties.setProperty("s3_access_key_id", config.getS3AccessKeyId());
        properties.setProperty("s3_secret_access_key", config.getS3SecretAccessKey());
        properties.setProperty("threads", "1");
        this.conn =  (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:", properties);
        final String statement = String.format(
                "ATTACH IF NOT EXISTS 'ducklake:%s' AS lake (DATA_PATH '%s');",
                config.getDucklakeCatalogUri(),
                config.getDataPath()

        );
        conn.createStatement().execute(statement);
    }

    public DuckDBConnection getConnection() {
        return conn;
    }

}
