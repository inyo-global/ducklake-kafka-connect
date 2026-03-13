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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.VectorBatchAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consolidates Arrow batches for efficient writing. Groups contiguous batches with compatible
 * schemas and merges each group into a single batch via in-place append.
 */
class BatchConsolidator {

  private static final Logger LOG = LoggerFactory.getLogger(BatchConsolidator.class);

  private BatchConsolidator() {}

  /**
   * Consolidates a list of batches into the minimum number of batches while preserving order.
   *
   * <p>Groups contiguous batches with compatible schemas into runs and merges each run via in-place
   * append into the first batch of the run. Incompatible batches start a new run.
   *
   * <p><b>Ownership contract:</b> After this method returns, the caller must only use the returned
   * list. Source batches consumed during consolidation (indices 1..N within each merged run) are
   * closed internally. The returned list may contain original batch instances (for single-batch
   * runs) or mutated first-batch instances (for merged runs). The caller must close every root in
   * the returned list.
   *
   * <p><b>On failure:</b> If an exception is thrown, some source batches may have been partially
   * consumed (closed) and the target batch may be in a corrupt state. The caller must close all
   * original input batches to avoid leaking Arrow memory; double-close is safe.
   *
   * @param batches the input batches; must not be modified by the caller after this call
   * @return a list of roots to write and close; never null
   * @throws RuntimeException if in-place append fails (e.g. OOM during vector reallocation)
   */
  static List<VectorSchemaRoot> consolidate(List<VectorSchemaRoot> batches) {
    if (batches == null || batches.isEmpty()) {
      return Collections.emptyList();
    }
    if (batches.size() == 1) {
      return List.of(batches.get(0));
    }

    List<List<VectorSchemaRoot>> runs = groupContiguousRuns(batches);
    List<VectorSchemaRoot> result = new ArrayList<>(runs.size());

    for (List<VectorSchemaRoot> run : runs) {
      if (run.size() == 1) {
        result.add(run.get(0));
      } else {
        result.add(consolidateRun(run));
      }
    }

    if (runs.size() > 1) {
      LOG.info("Grouped {} batches into {} contiguous runs by schema", batches.size(), runs.size());
    }

    return result;
  }

  /**
   * Groups batches into contiguous runs of compatible schemas. Walks the list in order; starts a
   * new run whenever the schema changes.
   */
  private static List<List<VectorSchemaRoot>> groupContiguousRuns(List<VectorSchemaRoot> batches) {
    List<List<VectorSchemaRoot>> runs = new ArrayList<>();
    List<VectorSchemaRoot> currentRun = new ArrayList<>();
    currentRun.add(batches.get(0));

    for (int i = 1; i < batches.size(); i++) {
      Schema prevSchema = currentRun.get(0).getSchema();
      Schema currSchema = batches.get(i).getSchema();
      if (schemasAreCompatible(prevSchema, currSchema)) {
        currentRun.add(batches.get(i));
      } else {
        runs.add(currentRun);
        currentRun = new ArrayList<>();
        currentRun.add(batches.get(i));
      }
    }
    runs.add(currentRun);
    return runs;
  }

  /**
   * Consolidates a run of same-schema batches by appending in-place into the first batch. Closes
   * consumed batches (index 1..N). Returns the first batch with all data appended. Caller must
   * ensure run.size() > 1.
   */
  private static VectorSchemaRoot consolidateRun(List<VectorSchemaRoot> run) {
    VectorSchemaRoot target = run.get(0);
    int totalRows = run.stream().mapToInt(VectorSchemaRoot::getRowCount).sum();

    for (int i = 0; i < target.getFieldVectors().size(); i++) {
      FieldVector targetVector = target.getFieldVectors().get(i);
      FieldVector[] sourceVectors = new FieldVector[run.size() - 1];
      for (int j = 1; j < run.size(); j++) {
        sourceVectors[j - 1] = run.get(j).getFieldVectors().get(i);
      }
      VectorBatchAppender.batchAppend(targetVector, sourceVectors);
    }
    target.setRowCount(totalRows);

    // Close consumed source batches
    for (int i = 1; i < run.size(); i++) {
      try {
        run.get(i).close();
      } catch (Exception e) {
        LOG.warn("Failed to close consumed batch: {}", e.getMessage());
      }
    }

    return target;
  }

  /**
   * Checks if two schemas are compatible for consolidation. Compatible means same field count, and
   * each field has the same name, type, nullability, and children. Metadata differences are
   * ignored.
   */
  static boolean schemasAreCompatible(Schema schema1, Schema schema2) {
    if (schema1 == schema2) {
      return true;
    }
    List<Field> fields1 = schema1.getFields();
    List<Field> fields2 = schema2.getFields();
    if (fields1.size() != fields2.size()) {
      return false;
    }
    for (int i = 0; i < fields1.size(); i++) {
      if (!fieldsAreCompatible(fields1.get(i), fields2.get(i))) {
        return false;
      }
    }
    return true;
  }

  private static boolean fieldsAreCompatible(Field f1, Field f2) {
    if (!f1.getName().equals(f2.getName())) {
      return false;
    }
    if (!f1.getType().equals(f2.getType())) {
      return false;
    }
    if (f1.isNullable() != f2.isNullable()) {
      return false;
    }
    List<Field> children1 = f1.getChildren();
    List<Field> children2 = f2.getChildren();
    if (children1.size() != children2.size()) {
      return false;
    }
    for (int i = 0; i < children1.size(); i++) {
      if (!fieldsAreCompatible(children1.get(i), children2.get(i))) {
        return false;
      }
    }
    return true;
  }
}
