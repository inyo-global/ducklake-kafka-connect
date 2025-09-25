package com.inyo.ducklake.connect;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

public class TimestampDebugTest {

    private RootAllocator allocator;
    private SinkRecordToArrowConverter converter;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
        converter = new SinkRecordToArrowConverter(allocator);
    }

    @AfterEach
    void tearDown() {
        if (converter != null) {
            converter.close();
        }
        if (allocator != null) {
            allocator.close();
        }
    }

    @Test
    void debugTimestampProcessing() {
        // Create the same test data as the failing test
        List<SinkRecord> records = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            Map<String, Object> mapValue = new HashMap<>();
            mapValue.put("id", i + 1);
            mapValue.put("created_at", "2025-09-25T1" + (8 + i) + ":05:12Z");
            mapValue.put("name", "Record " + (i + 1));

            System.out.println("Creating record " + i + " with timestamp: " + mapValue.get("created_at"));
            System.out.println("TimestampUtils.isTimestamp result: " +
                TimestampUtils.isTimestamp((String) mapValue.get("created_at")));

            records.add(new SinkRecord(
                "test-topic", 0, null, null, null, mapValue, i
            ));
        }

        try (VectorSchemaRoot result = converter.convertRecords(records)) {
            System.out.println("Schema: " + result.getSchema());

            TimeStampMilliVector timestampVector = (TimeStampMilliVector) result.getVector("created_at");

            for (int i = 0; i < 3; i++) {
                System.out.println("Row " + i + ": isNull=" + timestampVector.isNull(i) +
                    ", value=" + (timestampVector.isNull(i) ? "null" : timestampVector.get(i)));
            }
        }
    }
}
