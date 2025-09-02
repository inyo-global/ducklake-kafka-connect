package com.inyo.ducklake.ingestor;

import java.util.Map;

public class DucklakeIngestor {

    private final Map<String, DucklakeWriter> writers;

    public DucklakeIngestor(Map<String, DucklakeWriter> writers) {
        this.writers = writers;
    }
}
