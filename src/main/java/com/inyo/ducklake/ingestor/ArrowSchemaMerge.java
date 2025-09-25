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
package com.inyo.ducklake.ingestor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Class for merging multiple Arrow schemas, similar to PyArrow's unify_schema functionality.
 * Handles field merging, type unification, and schema compatibility checking.
 */
public class ArrowSchemaMerge {

  /**
   * Unifies multiple Arrow schemas into a single schema. Fields with the same name are merged, and
   * type conflicts are resolved.
   *
   * @param schemas List of schemas to merge
   * @return Unified schema containing all fields from input schemas
   * @throws IllegalArgumentException if schemas cannot be merged due to incompatible types
   */
  public static Schema unifySchemas(List<Schema> schemas) {
    if (schemas == null || schemas.isEmpty()) {
      throw new IllegalArgumentException("Schema list cannot be null or empty");
    }

    if (schemas.size() == 1) {
      return schemas.get(0);
    }

    Map<String, List<Field>> fieldsByName = new HashMap<>();

    // Group fields by name across all schemas
    for (Schema schema : schemas) {
      for (Field field : schema.getFields()) {
        fieldsByName.computeIfAbsent(field.getName(), k -> new ArrayList<>()).add(field);
      }
    }

    // Merge fields with the same name
    List<Field> unifiedFields = new ArrayList<>();
    for (Map.Entry<String, List<Field>> entry : fieldsByName.entrySet()) {
      String fieldName = entry.getKey();
      List<Field> fields = entry.getValue();

      Field unifiedField = mergeFields(fieldName, fields);
      unifiedFields.add(unifiedField);
    }

    // Sort fields by name for consistent ordering
    unifiedFields.sort(Comparator.comparing(Field::getName));

    return new Schema(unifiedFields);
  }

  /**
   * Merges multiple fields with the same name into a single field.
   *
   * @param fieldName Name of the field
   * @param fields List of fields to merge
   * @return Merged field
   */
  private static Field mergeFields(String fieldName, List<Field> fields) {
    if (fields.size() == 1) {
      return fields.get(0);
    }

    // Determine if the merged field should be nullable
    boolean isNullable = fields.stream().anyMatch(Field::isNullable);

    // Get all unique types
    Set<ArrowType> types =
        fields.stream()
            .map(Field::getFieldType)
            .map(FieldType::getType)
            .collect(Collectors.toSet());

    ArrowType unifiedType;
    if (types.size() == 1) {
      // All fields have the same type
      unifiedType = types.iterator().next();
    } else {
      // Need to unify different types
      unifiedType = unifyTypes(new ArrayList<>(types));
    }

    // Merge children for complex types
    List<Field> unifiedChildren = null;
    if (isComplexType(unifiedType)) {
      unifiedChildren = mergeChildren(fields);
    }

    FieldType unifiedFieldType = new FieldType(isNullable, unifiedType, null);
    return new Field(fieldName, unifiedFieldType, unifiedChildren);
  }

  /**
   * Unifies different Arrow types into a compatible type. Follows type promotion rules similar to
   * PyArrow.
   *
   * @param types List of types to unify
   * @return Unified type
   */
  private static ArrowType unifyTypes(List<ArrowType> types) {
    if (types.size() == 1) {
      return types.get(0);
    }

    // Remove duplicates
    Set<ArrowType> uniqueTypes = new HashSet<>(types);
    if (uniqueTypes.size() == 1) {
      return uniqueTypes.iterator().next();
    }

    // Handle timestamp types specially - prefer timestamp over string
    if (areAllTimestampLike(uniqueTypes)) {
      return promoteTimestampTypes(uniqueTypes);
    }

    // Handle numeric type promotion
    if (areAllNumeric(uniqueTypes)) {
      return promoteNumericTypes(uniqueTypes);
    }

    // Handle string-like types
    if (areAllStringLike(uniqueTypes)) {
      return ArrowType.Utf8.INSTANCE; // Promote to UTF8
    }

    // Handle binary-like types
    if (areAllBinaryLike(uniqueTypes)) {
      return ArrowType.Binary.INSTANCE;
    }

    // Handle collection types
    if (areAllLists(uniqueTypes)) {
      return ArrowType.List.INSTANCE;
    }

    if (areAllMaps(uniqueTypes)) {
      return new ArrowType.Map(true); // Assume nullable maps
    }

    if (areAllStructs(uniqueTypes)) {
      return ArrowType.Struct.INSTANCE;
    }

    // If types cannot be unified, throw an exception
    throw new IllegalArgumentException("Cannot unify incompatible types: " + uniqueTypes);
  }

  /** Promotes numeric types to the most general type. */
  private static ArrowType promoteNumericTypes(Set<ArrowType> types) {
    boolean hasFloatingPoint = types.stream().anyMatch(t -> t instanceof ArrowType.FloatingPoint);
    boolean hasInt64 =
        types.stream()
            .anyMatch(
                t ->
                    t instanceof ArrowType.Int
                        && ((ArrowType.Int) t).getBitWidth() == ArrowTypeConstants.INT64_BIT_WIDTH);
    boolean hasInt32 =
        types.stream()
            .anyMatch(
                t ->
                    t instanceof ArrowType.Int
                        && ((ArrowType.Int) t).getBitWidth() == ArrowTypeConstants.INT32_BIT_WIDTH);

    if (hasFloatingPoint) {
      // Check if we have double precision
      boolean hasDouble =
          types.stream()
              .anyMatch(
                  t ->
                      t instanceof ArrowType.FloatingPoint
                          && ((ArrowType.FloatingPoint) t).getPrecision()
                              == FloatingPointPrecision.DOUBLE);

      if (hasDouble) {
        return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
      } else {
        return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
      }
    }

    if (hasInt64) {
      return new ArrowType.Int(ArrowTypeConstants.INT64_BIT_WIDTH, true);
    }

    if (hasInt32) {
      return new ArrowType.Int(ArrowTypeConstants.INT32_BIT_WIDTH, true);
    }

    // Default to int32 for smaller integer types
    return new ArrowType.Int(ArrowTypeConstants.INT32_BIT_WIDTH, true);
  }

  /** Promotes timestamp types to the most general type. */
  private static ArrowType promoteTimestampTypes(Set<ArrowType> types) {
    // If there is a timestamp among the types, prefer timestamp
    boolean hasTimestamp = types.stream().anyMatch(t -> t instanceof ArrowType.Timestamp);

    if (hasTimestamp) {
      // Find the timestamp with the highest precision
      return types.stream()
          .filter(t -> t instanceof ArrowType.Timestamp)
          .map(t -> (ArrowType.Timestamp) t)
          .findFirst()
          .orElse(new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, null));
    }

    // If there is no timestamp, but there is time, promote to time
    boolean hasTime = types.stream().anyMatch(t -> t instanceof ArrowType.Time);
    if (hasTime) {
      return types.stream()
          .filter(t -> t instanceof ArrowType.Time)
          .map(t -> (ArrowType.Time) t)
          .findFirst()
          .orElse(new ArrowType.Time(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, 32));
    }

    // Fallback
    return new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, null);
  }

  /** Merges children from complex type fields. */
  private static List<Field> mergeChildren(List<Field> fields) {
    Map<String, List<Field>> childrenByName = new HashMap<>();

    // Collect all children by name
    for (Field field : fields) {
      if (field.getChildren() != null) {
        for (Field child : field.getChildren()) {
          childrenByName.computeIfAbsent(child.getName(), k -> new ArrayList<>()).add(child);
        }
      }
    }

    // Merge children with the same name
    List<Field> mergedChildren = new ArrayList<>();
    for (Map.Entry<String, List<Field>> entry : childrenByName.entrySet()) {
      String childName = entry.getKey();
      List<Field> children = entry.getValue();
      Field mergedChild = mergeFields(childName, children);
      mergedChildren.add(mergedChild);
    }

    mergedChildren.sort(Comparator.comparing(Field::getName));
    return mergedChildren;
  }

  // Type checking helper methods

  private static boolean isComplexType(ArrowType type) {
    return type instanceof ArrowType.Struct
        || type instanceof ArrowType.List
        || type instanceof ArrowType.Map;
  }

  private static boolean areAllNumeric(Set<ArrowType> types) {
    return types.stream()
        .allMatch(
            t ->
                t instanceof ArrowType.Int
                    || t instanceof ArrowType.FloatingPoint
                    || t instanceof ArrowType.Bool);
  }

  private static boolean areAllStringLike(Set<ArrowType> types) {
    return types.stream()
        .allMatch(t -> t instanceof ArrowType.Utf8
                    || t instanceof ArrowType.LargeUtf8);
  }

  private static boolean areAllBinaryLike(Set<ArrowType> types) {
    return types.stream()
        .allMatch(t -> t instanceof ArrowType.Binary || t instanceof ArrowType.LargeBinary);
  }

  private static boolean areAllLists(Set<ArrowType> types) {
    return types.stream().allMatch(t -> t instanceof ArrowType.List);
  }

  private static boolean areAllMaps(Set<ArrowType> types) {
    return types.stream().allMatch(t -> t instanceof ArrowType.Map);
  }

  private static boolean areAllStructs(Set<ArrowType> types) {
    return types.stream().allMatch(t -> t instanceof ArrowType.Struct);
  }

  private static boolean areAllTimestampLike(Set<ArrowType> types) {
    return types.stream().allMatch(t ->
        t instanceof ArrowType.Timestamp ||
        t instanceof ArrowType.Time ||
        t instanceof ArrowType.Date ||
        t instanceof ArrowType.Utf8); // Allow string as compatible with timestamp
  }

  /**
   * Checks if two schemas are compatible (can be merged without data loss).
   *
   * @param schema1 First schema
   * @param schema2 Second schema
   * @return true if schemas are compatible
   */
  public static boolean areCompatible(Schema schema1, Schema schema2) {
    try {
      unifySchemas(Arrays.asList(schema1, schema2));
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  /**
   * Creates a new schema by adding a field to an existing schema. If a field with the same name
   * exists, it will be merged.
   *
   * @param schema Existing schema
   * @param newField Field to add
   * @return New schema with the added field
   */
  public static Schema addField(Schema schema, Field newField) {
    List<Field> existingFields = new ArrayList<>(schema.getFields());

    // Check if field already exists
    Optional<Field> existingField =
        existingFields.stream().filter(f -> f.getName().equals(newField.getName())).findFirst();

    if (existingField.isPresent()) {
      // Merge with existing field
      existingFields.remove(existingField.get());
      Field mergedField =
          mergeFields(newField.getName(), Arrays.asList(existingField.get(), newField));
      existingFields.add(mergedField);
    } else {
      // Add new field
      existingFields.add(newField);
    }

    existingFields.sort(Comparator.comparing(Field::getName));
    return new Schema(existingFields);
  }

  /**
   * Removes a field from a schema by name.
   *
   * @param schema Existing schema
   * @param fieldName Name of field to remove
   * @return New schema without the specified field
   */
  public static Schema removeField(Schema schema, String fieldName) {
    List<Field> filteredFields =
        schema.getFields().stream()
            .filter(f -> !f.getName().equals(fieldName))
            .collect(Collectors.toList());

    return new Schema(filteredFields);
  }
}
