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

import java.util.Arrays;

/**
 * Configuration record for DucklakeWriter containing all necessary settings for writing data to
 * DuckDB tables.
 *
 * @param destinationTable the destination table name where data will be written
 * @param autoCreateTable whether the table should be automatically created if it doesn't exist
 * @param tableIdColumns array of column names that serve as ID/primary key columns for the table
 * @param partitionByColumns array of column names used for partitioning the table
 */
public record DucklakeWriterConfig(
    String destinationTable,
    boolean autoCreateTable,
    String[] tableIdColumns,
    String[] partitionByColumns) {

  /** Compact constructor that validates and clones arrays to ensure immutability. */
  public DucklakeWriterConfig {
    tableIdColumns = tableIdColumns != null ? tableIdColumns.clone() : new String[0];
    partitionByColumns = partitionByColumns != null ? partitionByColumns.clone() : new String[0];
  }

  /**
   * Gets the column names that serve as ID/primary key columns for the table.
   *
   * @return array of ID column names (cloned for immutability)
   */
  public String[] tableIdColumns() {
    return tableIdColumns.clone();
  }

  /**
   * Gets the column names used for partitioning the table.
   *
   * @return array of partition column names (cloned for immutability)
   */
  public String[] partitionByColumns() {
    return partitionByColumns.clone();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    DucklakeWriterConfig that = (DucklakeWriterConfig) obj;
    return autoCreateTable == that.autoCreateTable
        && destinationTable.equals(that.destinationTable)
        && Arrays.equals(tableIdColumns, that.tableIdColumns)
        && Arrays.equals(partitionByColumns, that.partitionByColumns);
  }
}
