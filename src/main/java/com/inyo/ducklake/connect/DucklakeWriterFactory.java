package com.inyo.ducklake.connect;

import com.inyo.ducklake.ingestor.DucklakeWriter;
import org.duckdb.DuckDBConnection;

import java.sql.SQLException;

public class DucklakeWriterFactory {

    private final DucklakeSinkConfig config;
    private final DuckDBConnection conn;

    public DucklakeWriterFactory(
            DucklakeSinkConfig config,
            DuckDBConnection conn
    ) {
        this.config = config;
        this.conn = conn;
    }

    public DucklakeWriter create(String topic) {
        final var tables = config.getTopicToTableMap();
        final var table = tables.getOrDefault(topic, topic);
        return new DucklakeWriter(conn, table);
    }
}
