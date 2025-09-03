package com.inyo.ducklake.connect;

import com.inyo.ducklake.ingestor.ArrowSchemaMerge;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Converts Kafka SinkRecords to Arrow VectorSchemaRoot with unified schema.
 * Handles schema unification, type conversion, and data population.
 */
public class SinkRecordToArrowConverter {
    private static final System.Logger LOG = System.getLogger(SinkRecordToArrowConverter.class.getName());

    private final BufferAllocator allocator;

    public SinkRecordToArrowConverter(BufferAllocator allocator) {
        this.allocator = allocator;
    }

    /**
     * Converts a collection of SinkRecords to VectorSchemaRoot with unified schema.
     *
     * @param records Collection of SinkRecords to convert
     * @return VectorSchemaRoot containing the converted data
     * @throws RuntimeException if conversion fails
     */
    public VectorSchemaRoot convertRecords(Collection<SinkRecord> records) {
        if (records == null || records.isEmpty()) {
            throw new IllegalArgumentException("Records collection cannot be null or empty");
        }

        try {
            // Convert Kafka schemas to Arrow schemas
            List<Schema> arrowSchemas = extractArrowSchemas(records);

            if (arrowSchemas.isEmpty()) {
                throw new IllegalStateException("No valid schemas found in records");
            }

            // Unify all schemas into a single schema
            Schema unifiedSchema = unifySchemas(arrowSchemas);

            // Create and populate VectorSchemaRoot
            VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(unifiedSchema, allocator);
            populateVectors(vectorSchemaRoot, records, unifiedSchema);
            vectorSchemaRoot.setRowCount(records.size());

            LOG.log(System.Logger.Level.INFO,
                "Converted {0} records to VectorSchemaRoot with unified schema: {1}",
                records.size(), unifiedSchema);

            return vectorSchemaRoot;
        } catch (Exception e) {
            LOG.log(System.Logger.Level.ERROR, "Error converting records to Arrow format", e);
            throw new RuntimeException("Failed to convert SinkRecords to VectorSchemaRoot", e);
        }
    }

    /**
     * Converts records grouped by partition to VectorSchemaRoot.
     *
     * @param recordsByPartition Map of records grouped by TopicPartition
     * @return Map of VectorSchemaRoot for each partition
     */
    public Map<TopicPartition, VectorSchemaRoot> convertRecordsByPartition(
            Map<TopicPartition, List<SinkRecord>> recordsByPartition) {

        Map<TopicPartition, VectorSchemaRoot> result = new HashMap<>();

        for (Map.Entry<TopicPartition, List<SinkRecord>> entry : recordsByPartition.entrySet()) {
            TopicPartition partition = entry.getKey();
            List<SinkRecord> partitionRecords = entry.getValue();

            if (!partitionRecords.isEmpty()) {
                try {
                    VectorSchemaRoot vectorSchemaRoot = convertRecords(partitionRecords);
                    result.put(partition, vectorSchemaRoot);

                    LOG.log(System.Logger.Level.INFO,
                        "Converted partition {0} with {1} records",
                        partition, partitionRecords.size());
                } catch (Exception e) {
                    LOG.log(System.Logger.Level.ERROR,
                        "Failed to convert records for partition: " + partition, e);
                    throw new RuntimeException("Failed to convert partition records: " + partition, e);
                }
            }
        }

        return result;
    }

    /**
     * Groups SinkRecords by TopicPartition.
     *
     * @param records Collection of SinkRecords
     * @return Map of records grouped by TopicPartition
     */
    public static Map<TopicPartition, List<SinkRecord>> groupRecordsByPartition(Collection<SinkRecord> records) {
        return records.stream()
                .collect(Collectors.groupingBy(record ->
                    new TopicPartition(record.topic(), record.kafkaPartition())));
    }

    private List<Schema> extractArrowSchemas(Collection<SinkRecord> records) {
        return records.stream()
                .filter(record -> record.valueSchema() != null)
                .map(record -> KafkaSchemaToArrow.arrowSchemaFromKafka(record.valueSchema()))
                .distinct()
                .collect(Collectors.toList());
    }

    private Schema unifySchemas(List<Schema> arrowSchemas) {
        if (arrowSchemas.size() == 1) {
            return arrowSchemas.get(0);
        }
        return ArrowSchemaMerge.unifySchemas(arrowSchemas);
    }

    private void populateVectors(VectorSchemaRoot root, Collection<SinkRecord> records, Schema schema) {
        // Initialize all vectors
        root.allocateNew();

        // Create mapping of field names to vectors
        Map<String, FieldVector> vectorMap = createVectorMap(root, schema);

        // Populate data row by row
        int rowIndex = 0;
        for (SinkRecord record : records) {
            if (record.value() instanceof Struct) {
                populateStructData(vectorMap, (Struct) record.value(), rowIndex);
            } else {
                LOG.log(System.Logger.Level.WARNING,
                    "Unsupported record value type at row {0}: {1}",
                    rowIndex, record.value() != null ? record.value().getClass() : "null");

                // Set all fields to null for unsupported types
                setRowToNull(vectorMap, rowIndex);
            }
            rowIndex++;
        }
    }

    private Map<String, FieldVector> createVectorMap(VectorSchemaRoot root, Schema schema) {
        Map<String, FieldVector> vectorMap = new HashMap<>();
        for (Field field : schema.getFields()) {
            vectorMap.put(field.getName(), root.getVector(field.getName()));
        }
        return vectorMap;
    }

    private void populateStructData(Map<String, FieldVector> vectorMap, Struct struct, int rowIndex) {
        // Handle fields that exist in the struct
        for (org.apache.kafka.connect.data.Field kafkaField : struct.schema().fields()) {
            String fieldName = kafkaField.name();
            Object value = struct.get(fieldName);

            FieldVector vector = vectorMap.get(fieldName);
            if (vector != null) {
                setVectorValue(vector, rowIndex, value, kafkaField.schema().type());
            }
        }

        // Handle fields that exist in unified schema but not in this struct (set to null)
        Set<String> structFieldNames = struct.schema().fields().stream()
                .map(org.apache.kafka.connect.data.Field::name)
                .collect(Collectors.toSet());

        for (String vectorFieldName : vectorMap.keySet()) {
            if (!structFieldNames.contains(vectorFieldName)) {
                vectorMap.get(vectorFieldName).setNull(rowIndex);
            }
        }
    }

    private void setRowToNull(Map<String, FieldVector> vectorMap, int rowIndex) {
        for (FieldVector vector : vectorMap.values()) {
            vector.setNull(rowIndex);
        }
    }

    private void setVectorValue(FieldVector vector, int index, Object value,
                               org.apache.kafka.connect.data.Schema.Type type) {
        if (value == null) {
            vector.setNull(index);
            return;
        }

        try {
            switch (type) {
                case INT8:
                    ((TinyIntVector) vector).set(index, ((Number) value).byteValue());
                    break;
                case INT16:
                    ((SmallIntVector) vector).set(index, ((Number) value).shortValue());
                    break;
                case INT32:
                    ((IntVector) vector).set(index, ((Number) value).intValue());
                    break;
                case INT64:
                    ((BigIntVector) vector).set(index, ((Number) value).longValue());
                    break;
                case FLOAT32:
                    ((Float4Vector) vector).set(index, ((Number) value).floatValue());
                    break;
                case FLOAT64:
                    ((Float8Vector) vector).set(index, ((Number) value).doubleValue());
                    break;
                case BOOLEAN:
                    ((BitVector) vector).set(index, (Boolean) value ? 1 : 0);
                    break;
                case STRING:
                    ((VarCharVector) vector).set(index, value.toString().getBytes());
                    break;
                case BYTES:
                    if (value instanceof byte[]) {
                        ((VarBinaryVector) vector).set(index, (byte[]) value);
                    } else {
                        LOG.log(System.Logger.Level.WARNING, "Expected byte[] for BYTES type, got: {0}", value.getClass());
                        vector.setNull(index);
                    }
                    break;
                case STRUCT:
                    // TODO: Handle nested struct types
                    LOG.log(System.Logger.Level.WARNING, "Nested STRUCT types not yet supported");
                    vector.setNull(index);
                    break;
                case ARRAY:
                    // TODO: Handle array types
                    LOG.log(System.Logger.Level.WARNING, "ARRAY types not yet supported");
                    vector.setNull(index);
                    break;
                case MAP:
                    // TODO: Handle map types
                    LOG.log(System.Logger.Level.WARNING, "MAP types not yet supported");
                    vector.setNull(index);
                    break;
                default:
                    LOG.log(System.Logger.Level.WARNING, "Unsupported field type for vector population: {0}", type);
                    vector.setNull(index);
            }
        } catch (Exception e) {
            LOG.log(System.Logger.Level.ERROR,
                "Error setting vector value at index {0} for type {1}: {2}",
                index, type, e.getMessage());
            vector.setNull(index);
        }
    }
}
