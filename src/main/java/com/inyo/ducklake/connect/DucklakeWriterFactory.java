package com.inyo.ducklake.connect;

import com.inyo.ducklake.ingestor.DucklakeWriter;
import com.inyo.ducklake.ingestor.DucklakeWriterConfig;
import org.duckdb.DuckDBConnection;

import java.sql.SQLException;

public final class DucklakeWriterFactory {

    private final DucklakeSinkConfig config;
    private final DuckDBConnection conn;

    public DucklakeWriterFactory(
            DucklakeSinkConfig config,
            DuckDBConnection conn
    ) {
        this.config = config;
        try {
            this.conn = (DuckDBConnection) conn.duplicate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public DucklakeWriter create(String topic) {
        final var topicsTables = config.getTopicToTableMap();
        final var table = topicsTables.getOrDefault(topic, topic);

        final var idCols = config.getTableIdColumns(table);
        final var partitionCols = config.getTablePartitionByColumns(table);
        final var autoCreate = config.getTableAutoCreate(table);

        DucklakeWriterConfig writerConfig = new DucklakeWriterConfig(
                table,
                autoCreate,
                idCols,
                partitionCols
        );

        return new DucklakeWriter(conn, writerConfig);
    }
}
