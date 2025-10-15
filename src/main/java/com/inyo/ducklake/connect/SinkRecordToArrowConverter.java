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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.inyo.ducklake.ingestor.ArrowSchemaMerge;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Converts Kafka SinkRecords to Arrow VectorSchemaRoot with unified schema. Handles schema
 * unification, type conversion, and data population.
 */
public final class SinkRecordToArrowConverter implements AutoCloseable {
  private static final System.Logger LOG =
      System.getLogger(SinkRecordToArrowConverter.class.getName());

  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  private final BufferAllocator allocator;

  // not closed here

  public SinkRecordToArrowConverter(BufferAllocator externalAllocator) {
    if (externalAllocator == null) {
      throw new IllegalArgumentException("Allocator cannot be null");
    }
    // Create a child allocator to avoid exposing internal representation / accidental over-release
    this.allocator =
        externalAllocator.newChildAllocator("sink-record-converter", 0, Long.MAX_VALUE);
  }

  @Override
  public void close() {
    try {
      allocator.close();
    } catch (Exception e) {
      LOG.log(
          System.Logger.Level.WARNING, "Failed to close converter allocator: {0}", e.getMessage());
    }
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
      // Preprocess records: handle schemaless JSON strings/maps by inferring a Connect schema
      records =
          records.stream()
              .map(
                  r1 -> {
                    if (r1.valueSchema() != null) return r1;
                    Object val = r1.value();

                    // If the value is a JSON string, try to parse it into a Map
                    if (val instanceof String s) {
                      try {
                        Object parsed = JSON_MAPPER.readValue(s, new TypeReference<>() {});
                        if (parsed instanceof Map<?, ?> mParsed) {
                          val = mParsed;
                        } else {
                          return r1; // leave unchanged if not a map
                        }
                      } catch (IOException ex) {
                        return r1; // not JSON - leave unchanged
                      }
                    }

                    if (val instanceof Map<?, ?> mapVal) {
                      org.apache.kafka.connect.data.Schema inferred = inferSchemaFromObject(mapVal);
                      // Only create struct if we successfully inferred a schema (non-null result)
                      if (inferred != null) {
                        Struct struct = buildStructFromMap(mapVal, inferred);
                        return new SinkRecord(
                            r1.topic(),
                            r1.kafkaPartition(),
                            r1.keySchema(),
                            r1.key(),
                            inferred,
                            struct,
                            r1.kafkaOffset());
                      }
                    }
                    return r1;
                  })
              .collect(Collectors.toList());

      // Convert Kafka schemas to Arrow schemas
      List<Schema> arrowSchemas = extractArrowSchemas(records);

      if (arrowSchemas.isEmpty()) {
        throw new IllegalStateException("No valid schemas found in records");
      }

      // Unify all schemas into a single schema
      Schema unifiedSchema = unifySchemas(arrowSchemas, records);

      // Create and populate VectorSchemaRoot
      VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(unifiedSchema, allocator);
      populateVectors(vectorSchemaRoot, records, unifiedSchema);
      vectorSchemaRoot.setRowCount(records.size());

      LOG.log(
          System.Logger.Level.DEBUG,
          "Converted {0} records to VectorSchemaRoot with unified schema: {1}",
          records.size(),
          unifiedSchema);

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
        VectorSchemaRoot vectorSchemaRoot = null;
        try {
          vectorSchemaRoot = convertRecords(partitionRecords);
          result.put(partition, vectorSchemaRoot);

          LOG.log(
              System.Logger.Level.DEBUG,
              "Converted partition {0} with {1} records",
              partition,
              partitionRecords.size());
        } catch (Exception e) {
          // Ensure cleanup if conversion fails
          if (vectorSchemaRoot != null) {
            try {
              vectorSchemaRoot.close();
            } catch (Exception closeException) {
              LOG.log(
                  System.Logger.Level.WARNING,
                  "Failed to close VectorSchemaRoot during exception cleanup for partition {0}: {1}",
                  partition,
                  closeException.getMessage());
            }
          }
          LOG.log(
              System.Logger.Level.ERROR,
              "Failed to convert records for partition: " + partition,
              e);
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
  public static Map<TopicPartition, List<SinkRecord>> groupRecordsByPartition(
      Collection<SinkRecord> records) {
    return records.stream()
        .collect(
            Collectors.groupingBy(
                record -> new TopicPartition(record.topic(), record.kafkaPartition())));
  }

  private List<Schema> extractArrowSchemas(Collection<SinkRecord> records) {
    return records.stream()
        .filter(record -> record.valueSchema() != null)
        .map(record -> KafkaSchemaToArrow.arrowSchemaFromKafka(record.valueSchema()))
        .distinct()
        .collect(Collectors.toList());
  }

  private Schema unifySchemas(List<Schema> arrowSchemas, Collection<SinkRecord> records) {
    if (arrowSchemas.size() == 1) {
      return arrowSchemas.get(0);
    }

    // Use the new unified approach that only collects sample values when needed
    return ArrowSchemaMerge.unifySchemas(arrowSchemas, () -> collectSampleValues(records));
  }

  /**
   * Collects sample values from records for each field to help with error messages. This method
   * collects diverse samples showing different types/schemas for each field, rather than just the
   * first few values.
   *
   * @param records Collection of sink records
   * @return Map of field names to sample values
   */
  private Map<String, List<Object>> collectSampleValues(Collection<SinkRecord> records) {
    Map<String, Map<String, Object>> fieldTypeSamples = new HashMap<>();

    for (SinkRecord record : records) {
      if (record.value() instanceof Struct struct) {
        collectStructSampleValuesWithTypes(struct, fieldTypeSamples, "");
      } else if (record.value() != null) {
        // Handle non-struct values
        var typeKey = record.value().getClass().getSimpleName();
        fieldTypeSamples
            .computeIfAbsent("root", k -> new HashMap<>())
            .putIfAbsent(typeKey, record.value());
      }
    }

    // Convert to the expected format, keeping one sample per unique type per field
    Map<String, List<Object>> sampleValues = new HashMap<>();
    for (Map.Entry<String, Map<String, Object>> entry : fieldTypeSamples.entrySet()) {
      var fieldName = entry.getKey();
      var typeSamples = entry.getValue();
      sampleValues.put(fieldName, new ArrayList<>(typeSamples.values()));
    }

    return sampleValues;
  }

  /**
   * Recursively collects sample values from a Struct, keeping one sample per type per field.
   *
   * @param struct The struct to extract values from
   * @param fieldTypeSamples Map to store sample values by field name and type
   * @param prefix Field name prefix for nested structures
   */
  private void collectStructSampleValuesWithTypes(
      Struct struct, Map<String, Map<String, Object>> fieldTypeSamples, String prefix) {
    for (org.apache.kafka.connect.data.Field field : struct.schema().fields()) {
      var fieldName = prefix.isEmpty() ? field.name() : prefix + "." + field.name();
      var value = struct.get(field);

      if (value != null) {
        var typeKey = getTypeKey(value);

        // Store the actual field value (not the entire struct) for the specific field
        fieldTypeSamples
            .computeIfAbsent(field.name(), k -> new HashMap<>())
            .putIfAbsent(typeKey, value);

        if (value instanceof Struct nestedStruct) {
          // Handle nested structs - recurse into the structure
          collectStructSampleValuesWithTypes(nestedStruct, fieldTypeSamples, fieldName);
        }
      }
    }
  }

  /**
   * Gets a type key for categorizing values by their type/schema. This helps identify different
   * schemas for the same field.
   */
  private String getTypeKey(Object value) {
    if (value instanceof Struct struct) {
      // For structs, include schema info to distinguish different struct schemas
      var schema = struct.schema();
      var fieldNames =
          schema.fields().stream()
              .map(org.apache.kafka.connect.data.Field::name)
              .sorted()
              .collect(Collectors.joining(","));
      return "Struct{" + fieldNames + "}";
    } else {
      return value.getClass().getSimpleName();
    }
  }

  private void populateVectors(
      VectorSchemaRoot root, Collection<SinkRecord> records, Schema schema) {
    // Calculate exact capacity needed based on record count
    var recordCount = records.size();

    // Allocate vectors with exact capacity - no growth expected
    for (FieldVector vector : root.getFieldVectors()) {
      allocateVectorWithCapacity(vector, recordCount);
    }

    // Create mapping of field names to vectors
    Map<String, FieldVector> vectorMap = createVectorMap(root, schema);

    // Populate data row by row
    int rowIndex = 0;
    for (SinkRecord record : records) {
      // Check if we need to reallocate (safety check)
      // Only check capacity if we have field vectors
      if (!root.getFieldVectors().isEmpty()
          && rowIndex >= root.getFieldVectors().get(0).getValueCapacity()) {
        LOG.log(
            System.Logger.Level.INFO,
            "Reallocating vectors due to capacity exceeded at row: {0}",
            rowIndex);

        for (FieldVector vector : root.getFieldVectors()) {
          vector.reAlloc();
        }
      }

      if (record.value() instanceof Struct) {
        populateStructData(vectorMap, (Struct) record.value(), rowIndex);
      } else {
        LOG.log(
            System.Logger.Level.WARNING,
            "Unsupported record value type at row {0}: {1}",
            rowIndex,
            record.value() != null ? record.value().getClass() : "null");

        // Set all fields to null for unsupported types
        setRowToNull(vectorMap, rowIndex);
      }
      rowIndex++;
    }
  }

  private void allocateVectorWithCapacity(FieldVector vector, int recordCount) {
    if (vector instanceof BaseVariableWidthVector) {
      // For variable-width vectors (strings, binary), estimate bytes needed for this specific field
      var estimatedBytesPerField = getEstimatedBytesPerField(vector, recordCount);
      ((BaseVariableWidthVector) vector).allocateNew(estimatedBytesPerField, recordCount);
    } else if (vector instanceof StructVector structVector) {
      // For struct vectors, allocate with exact capacity and recursively allocate child vectors
      structVector.allocateNew();
      if (structVector.getValueCapacity() < recordCount) {
        while (structVector.getValueCapacity() < recordCount) {
          structVector.reAlloc();
        }
      }
      // Recursively allocate child vectors
      for (FieldVector childVector : structVector.getChildrenFromFields()) {
        allocateVectorWithCapacity(childVector, recordCount);
      }
    } else {
      // For fixed-width vectors, allocate with exact capacity
      vector.allocateNew();
      // Resize to exact capacity if current capacity is less
      if (vector.getValueCapacity() < recordCount) {
        while (vector.getValueCapacity() < recordCount) {
          vector.reAlloc();
        }
      }
    }
  }

  private int getEstimatedBytesPerField(FieldVector vector, int recordCount) {
    // Get the field from the vector to determine its type
    var field = vector.getField();
    var bytesPerValue =
        switch (field.getType().getTypeID()) {
          case Utf8View, Utf8, LargeUtf8 -> 256; // Conservative estimate for string fields
          case BinaryView, Binary, LargeBinary -> 256; // Conservative estimate for binary fields
          default -> 64; // Default for other variable-width types
        };

    // For columnar format, estimate total bytes for this field across all records
    return Math.max(bytesPerValue * recordCount, 64 * 1024); // At least 64KB
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
        setVectorValue(vector, rowIndex, value, kafkaField.schema());
      }
    }
    // Handle fields that exist in unified schema but not in this struct (set to null)
    Set<String> structFieldNames =
        struct.schema().fields().stream()
            .map(org.apache.kafka.connect.data.Field::name)
            .collect(Collectors.toSet());
    for (Map.Entry<String, FieldVector> entry : vectorMap.entrySet()) {
      if (!structFieldNames.contains(entry.getKey())) {
        entry.getValue().setNull(rowIndex);
      }
    }
  }

  private void setRowToNull(Map<String, FieldVector> vectorMap, int rowIndex) {
    for (FieldVector vector : vectorMap.values()) {
      vector.setNull(rowIndex);
    }
  }

  private void setVectorValue(
      FieldVector vector,
      int index,
      Object value,
      org.apache.kafka.connect.data.Schema kafkaSchema) {
    if (value == null) {
      vector.setNull(index);
      return;
    }

    // Handle timestamp logical types
    if (kafkaSchema.name() != null
        && kafkaSchema.name().equals("org.apache.kafka.connect.data.Timestamp")) {
      if (vector instanceof TimeStampMilliVector timestampVector) {
        if (value instanceof Long longValue) {
          // Value is already in epoch millis
          timestampVector.set(index, longValue);
          return;
        } else if (value instanceof String stringValue) {
          // Always try to convert string values to timestamps for timestamp fields
          try {
            long epochMillis = TimestampUtils.parseTimestampToEpochMillis(stringValue);
            timestampVector.set(index, epochMillis);
            return;
          } catch (Exception e) {
            LOG.log(
                System.Logger.Level.WARNING, "Failed to parse timestamp string: {0}", stringValue);
            vector.setNull(index);
            return;
          }
        } else if (value instanceof Date dateValue) {
          // Handle java.util.Date
          timestampVector.set(index, dateValue.getTime());
          return;
        } else {
          LOG.log(
              System.Logger.Level.WARNING,
              "Unsupported value type for timestamp: {0}",
              value.getClass());
          vector.setNull(index);
          return;
        }
      } else {
        // Vector is not a timestamp vector, treat as string
        ((VarCharVector) vector).set(index, value.toString().getBytes(StandardCharsets.UTF_8));
        return;
      }
    }

    final var type = kafkaSchema.type();

    switch (type) {
      case INT8 -> ((TinyIntVector) vector).set(index, ((Number) value).byteValue());
      case INT16 -> ((SmallIntVector) vector).set(index, ((Number) value).shortValue());
      case INT32 -> ((IntVector) vector).set(index, ((Number) value).intValue());
      case INT64 -> {
        // Handle case where schema expects INT64 but vector is actually VarChar due to unification
        if (vector instanceof VarCharVector) {
          ((VarCharVector) vector).set(index, value.toString().getBytes(StandardCharsets.UTF_8));
        } else {
          ((BigIntVector) vector).set(index, ((Number) value).longValue());
        }
      }
      case FLOAT32 -> ((Float4Vector) vector).set(index, ((Number) value).floatValue());
      case FLOAT64 -> ((Float8Vector) vector).set(index, ((Number) value).doubleValue());
      case BOOLEAN -> ((BitVector) vector).set(index, (Boolean) value ? 1 : 0);
      case STRING -> {
        // Handle case where unified schema created a timestamp vector but Kafka schema is string
        if (vector instanceof TimeStampMilliVector
            && value instanceof String stringValue
            && TimestampUtils.isTimestamp(stringValue)) {
          try {
            long epochMillis = TimestampUtils.parseTimestampToEpochMillis(stringValue);
            ((TimeStampMilliVector) vector).set(index, epochMillis);
          } catch (Exception e) {
            LOG.log(
                System.Logger.Level.WARNING,
                "Failed to parse timestamp string in STRING case: {0}",
                stringValue);
            vector.setNull(index);
          }
        } else if (vector instanceof TimeStampMilliVector) {
          // Vector is timestamp but value is not a valid timestamp string, set to null
          LOG.log(
              System.Logger.Level.WARNING,
              "Cannot set non-timestamp value to timestamp vector: {0}",
              value);
          vector.setNull(index);
        } else if (vector instanceof VarCharVector) {
          ((VarCharVector) vector).set(index, value.toString().getBytes(StandardCharsets.UTF_8));
        } else {
          LOG.log(
              System.Logger.Level.WARNING,
              "Unexpected vector type for STRING: {0}",
              vector.getClass());
          vector.setNull(index);
        }
      }
      case BYTES -> {
        if (value instanceof byte[] bytes) {
          ((VarBinaryVector) vector).set(index, bytes);
        } else {
          LOG.log(
              System.Logger.Level.WARNING,
              "Expected byte[] for BYTES type, got: {0}",
              value.getClass());
          vector.setNull(index);
        }
      }
      case STRUCT -> handleStructValue(vector, index, value, kafkaSchema);
      case ARRAY -> handleArrayValue(vector, index, value, kafkaSchema);
      case MAP -> handleMapValue(vector, index, value, kafkaSchema);
      default -> {
        LOG.log(
            System.Logger.Level.WARNING, "Unsupported field type for vector population: {0}", type);
        vector.setNull(index);
      }
    }
  }

  private void handleStructValue(
      FieldVector vector,
      int index,
      Object value,
      org.apache.kafka.connect.data.Schema kafkaSchema) {
    if (!(value instanceof Struct struct) || !(vector instanceof StructVector structVector)) {
      LOG.log(System.Logger.Level.WARNING, "STRUCT value/vector mismatch at index {0}", index);
      vector.setNull(index);
      return;
    }
    structVector.setIndexDefined(index);
    for (org.apache.kafka.connect.data.Field childField : kafkaSchema.fields()) {
      Object childVal = struct.get(childField.name());
      FieldVector childVector = structVector.getChild(childField.name());
      if (childVector != null) {
        setVectorValue(childVector, index, childVal, childField.schema());
      }
    }
  }

  private void handleArrayValue(
      FieldVector vector,
      int index,
      Object value,
      org.apache.kafka.connect.data.Schema kafkaSchema) {
    if (!(vector instanceof ListVector listVector) || !(value instanceof Collection<?> coll)) {
      LOG.log(System.Logger.Level.WARNING, "ARRAY value/vector mismatch at index {0}", index);
      vector.setNull(index);
      return;
    }
    listVector.startNewValue(index);
    FieldVector dataVector = listVector.getDataVector();
    org.apache.kafka.connect.data.Schema elemSchema = kafkaSchema.valueSchema();
    int elementsAdded = 0;
    for (Object elem : coll) {
      int elemIndex = dataVector.getValueCount();
      setVectorValue(dataVector, elemIndex, elem, elemSchema);
      dataVector.setValueCount(elemIndex + 1);
      elementsAdded++;
    }
    listVector.endValue(index, elementsAdded);
  }

  private void handleMapValue(
      FieldVector vector,
      int index,
      Object value,
      org.apache.kafka.connect.data.Schema kafkaSchema) {
    if (!(vector instanceof MapVector mapVector) || !(value instanceof Map<?, ?> mapVal)) {
      LOG.log(System.Logger.Level.WARNING, "MAP value/vector mismatch at index {0}", index);
      vector.setNull(index);
      return;
    }
    mapVector.startNewValue(index);
    StructVector entryStruct = (StructVector) mapVector.getDataVector();
    org.apache.kafka.connect.data.Schema keySchema = kafkaSchema.keySchema();
    org.apache.kafka.connect.data.Schema valSchema = kafkaSchema.valueSchema();
    int pairsAdded = 0;
    for (var e : mapVal.entrySet()) {
      int entryIndex = entryStruct.getValueCount();
      entryStruct.setIndexDefined(entryIndex);
      FieldVector keyVector = entryStruct.getChild("key");
      FieldVector valVector = entryStruct.getChild("value");
      if (keyVector != null) {
        setVectorValue(keyVector, entryIndex, e.getKey(), keySchema);
      }
      if (valVector != null) {
        setVectorValue(valVector, entryIndex, e.getValue(), valSchema);
      }
      entryStruct.setValueCount(entryIndex + 1);
      pairsAdded++;
    }
    mapVector.endValue(index, pairsAdded);
  }

  // Infer a Kafka Connect schema for a given example object (Maps -> STRUCT, Collections -> ARRAY)
  private org.apache.kafka.connect.data.Schema inferSchemaFromObject(Object example) {
    if (example == null) {
      // Don't infer schema from null values - return null to indicate no type information
      return null;
    }
    if (example instanceof Map<?, ?> map) {
      SchemaBuilder sb = SchemaBuilder.struct();
      for (var e : map.entrySet()) {
        String name = String.valueOf(e.getKey());
        Object v = e.getValue();
        org.apache.kafka.connect.data.Schema childSchema = inferSchemaFromObject(v);
        // Only add fields where we can determine the schema from non-null values
        if (childSchema != null) {
          sb.field(name, childSchema);
        }
      }
      return sb.build();
    }
    if (example instanceof Collection<?> coll) {
      org.apache.kafka.connect.data.Schema elemSchema = null;
      for (Object o : coll) {
        if (o != null) {
          elemSchema = inferSchemaFromObject(o);
          if (elemSchema != null) {
            break;
          }
        }
      }
      // Only create array schema if we found a non-null element type
      if (elemSchema != null) {
        return SchemaBuilder.array(elemSchema).build();
      }
      // If all elements are null, we can't determine the array element type
      return null;
    }
    if (example instanceof Integer) return org.apache.kafka.connect.data.Schema.INT32_SCHEMA;
    if (example instanceof Long) return org.apache.kafka.connect.data.Schema.INT64_SCHEMA;
    if (example instanceof Short) return org.apache.kafka.connect.data.Schema.INT16_SCHEMA;
    if (example instanceof Byte) return org.apache.kafka.connect.data.Schema.INT8_SCHEMA;
    if (example instanceof Float) return org.apache.kafka.connect.data.Schema.FLOAT32_SCHEMA;
    if (example instanceof Double) return org.apache.kafka.connect.data.Schema.FLOAT64_SCHEMA;
    if (example instanceof Boolean) return org.apache.kafka.connect.data.Schema.BOOLEAN_SCHEMA;
    if (example instanceof byte[]) return org.apache.kafka.connect.data.Schema.BYTES_SCHEMA;

    if (example instanceof String str && TimestampUtils.isTimestamp(str)) {
      return Timestamp.builder().optional().build();
    }

    // default to string
    return org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
  }

  // Build a Struct from a Map using the provided schema (assumes schema is a STRUCT)
  private Struct buildStructFromMap(Map<?, ?> map, org.apache.kafka.connect.data.Schema schema) {
    if (schema == null || schema.type() != org.apache.kafka.connect.data.Schema.Type.STRUCT) {
      throw new IllegalArgumentException("Schema must be a STRUCT to build a Struct");
    }
    Struct struct = new Struct(schema);
    for (org.apache.kafka.connect.data.Field f : schema.fields()) {
      Object raw = map.get(f.name());
      if (raw == null) {
        struct.put(f.name(), null);
        continue;
      }

      // Handle timestamp conversion for string values
      if (f.schema().name() != null
          && f.schema().name().equals("org.apache.kafka.connect.data.Timestamp")) {
        if (raw instanceof String stringValue && TimestampUtils.isTimestamp(stringValue)) {
          try {
            long epochMillis = TimestampUtils.parseTimestampToEpochMillis(stringValue);
            struct.put(f.name(), new Date(epochMillis));
            continue; // Skip the switch statement
          } catch (Exception e) {
            LOG.log(
                System.Logger.Level.WARNING,
                "Failed to convert timestamp string for field {0}: {1}",
                f.name(),
                stringValue);
            struct.put(f.name(), null);
            continue; // Skip the switch statement
          }
        }
      }

      switch (f.schema().type()) {
        case STRUCT -> struct.put(f.name(), buildStructFromMap((Map<?, ?>) raw, f.schema()));
        case ARRAY -> {
          if (raw instanceof Collection<?> coll) {
            var elementSchema = f.schema().valueSchema();
            if (elementSchema.type() == org.apache.kafka.connect.data.Schema.Type.STRUCT) {
              // Convert Map elements to Structs if array contains struct elements
              var convertedList = new ArrayList<>();
              for (var element : coll) {
                if (element instanceof Map<?, ?> mapElement) {
                  convertedList.add(buildStructFromMap(mapElement, elementSchema));
                } else {
                  convertedList.add(element);
                }
              }
              struct.put(f.name(), convertedList);
            } else {
              struct.put(f.name(), coll);
            }
          } else {
            struct.put(f.name(), null);
          }
        }
        case MAP -> {
          if (raw instanceof Map<?, ?> m) {
            struct.put(f.name(), m);
          } else {
            struct.put(f.name(), null);
          }
        }
        default -> struct.put(f.name(), raw);
      }
    }
    return struct;
  }
}
