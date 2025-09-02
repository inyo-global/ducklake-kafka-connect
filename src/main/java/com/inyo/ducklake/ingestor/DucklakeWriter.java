package com.inyo.ducklake.ingestor;

import org.duckdb.DuckDBConnection;

public class DucklakeWriter {

    private final DuckDBConnection connection;
    private final String table;

    public DucklakeWriter(DuckDBConnection connection, String table) {
        this.connection = connection;
        this.table = table;
    }
}
