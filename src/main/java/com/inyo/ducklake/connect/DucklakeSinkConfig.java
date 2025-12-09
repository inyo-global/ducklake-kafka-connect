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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class DucklakeSinkConfig extends AbstractConfig {

  /** This class used iceberg-kafka-connect as reference implementation */
  public static final String VERSION = "1.0.0";

  // Data path validation patterns
  private static final Pattern S3_PATH_PATTERN = Pattern.compile("^s3a?://[a-zA-Z0-9._-]+(/.*)?$");
  private static final Pattern GCS_PATH_PATTERN = Pattern.compile("^gs://[a-zA-Z0-9._-]+(/.*)?$");
  private static final Pattern LOCAL_PATH_PATTERN = Pattern.compile("^file:///.+$");

  public static final String DUCKLAKE_CATALOG_URI = "ducklake.catalog_uri";
  public static final String DATA_INLINING_ROW_LIMIT = "data.inlining.row.limit";

  public static final ConfigDef CONFIG_DEF = newConfigDef();

  static final String DATA_PATH = "ducklake.data_path";
  static final String TOPICS_TABLES_MAP = "topic2table.map";
  static final String S3_URL_STYLE = "s3.url_style";
  static final String S3_USE_SSL = "s3.use_ssl";
  static final String S3_ENDPOINT = "s3.endpoint";
  static final String S3_ACCESS_KEY_ID = "s3.access_key_id";
  static final String S3_SECRET_ACCESS_KEY = "s3.secret_access_key";
  static final String CONSUMER_OVERRIDE_MAX_POLL_RECORDS = "consumer.override.max.poll.records";

  // Buffering configuration for larger file sizes
  public static final String FLUSH_SIZE = "flush.size";
  public static final String FLUSH_INTERVAL_MS = "flush.interval.ms";
  public static final String FILE_SIZE_BYTES = "file.size.bytes";

  // Performance tuning
  public static final String DUCKDB_THREADS = "duckdb.threads";
  public static final String PARALLEL_PARTITION_FLUSH = "parallel.partition.flush";

  // Table-specific configuration property patterns
  static final String TABLE_ID_COLUMNS_PATTERN = "ducklake.table.%s.id-columns";
  static final String TABLE_PARTITION_BY_PATTERN = "ducklake.table.%s.partition-by";
  static final String TABLE_AUTO_CREATE_PATTERN = "ducklake.table.%s.auto-create";

  private static ConfigDef newConfigDef() {
    return new ConfigDef()
        .define(
            DUCKLAKE_CATALOG_URI,
            ConfigDef.Type.PASSWORD,
            ConfigDef.Importance.HIGH,
            "Ducklake catalog URI, e.g., postgres:dbname=ducklake_catalog host=localhost")
        .define(
            DATA_INLINING_ROW_LIMIT,
            ConfigDef.Type.STRING,
            "off",
            ConfigDef.Importance.MEDIUM,
            "Maximum number of rows to inline into metadata for small files, or 'off' to disable")
        .define(
            TOPICS_TABLES_MAP,
            ConfigDef.Type.STRING,
            "",
            new TopicToTableValidator(),
            ConfigDef.Importance.LOW,
            "Map of topics to tables (optional). Format : comma-separated "
                + "tuples, e.g. <topic-1>:<table-1>,<topic-2>:<table-2>,...")
        .define(
            DATA_PATH,
            ConfigDef.Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            new DataPathValidator(),
            ConfigDef.Importance.HIGH,
            "Data path in the format eg s3://my-bucket/path/, gs://my-bucket/path/, file:///path/")
        .define(
            S3_URL_STYLE, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Either vhost or path")
        .define(
            S3_USE_SSL,
            ConfigDef.Type.STRING,
            ConfigDef.Importance.HIGH,
            "Whether to use HTTPS or HTTP")
        .define(
            S3_ENDPOINT,
            ConfigDef.Type.STRING,
            ConfigDef.Importance.HIGH,
            "Specify a custom S3 endpoint")
        .define(
            S3_ACCESS_KEY_ID,
            ConfigDef.Type.PASSWORD,
            ConfigDef.Importance.HIGH,
            "The ID of the key to use")
        .define(
            S3_SECRET_ACCESS_KEY,
            ConfigDef.Type.PASSWORD,
            ConfigDef.Importance.HIGH,
            "The secret of the key to use")
        .define(
            CONSUMER_OVERRIDE_MAX_POLL_RECORDS,
            ConfigDef.Type.INT,
            1024,
            ConfigDef.Importance.MEDIUM,
            "Maximum number of records returned in a single call to poll(). Defaults to 1024 if not specified.")
        .define(
            FLUSH_SIZE,
            ConfigDef.Type.INT,
            1000000,
            ConfigDef.Importance.MEDIUM,
            "Number of records to buffer before writing to DuckLake. "
                + "Larger values produce larger files but use more memory. Default: 1000000 (1M records)")
        .define(
            FLUSH_INTERVAL_MS,
            ConfigDef.Type.LONG,
            60000L,
            ConfigDef.Importance.MEDIUM,
            "Maximum time in milliseconds to buffer records before forcing a flush. "
                + "Ensures data is written even with low throughput. Default: 60000 (60 seconds)")
        .define(
            FILE_SIZE_BYTES,
            ConfigDef.Type.LONG,
            268435456L,
            ConfigDef.Importance.MEDIUM,
            "Target file size in bytes before flushing the buffer. "
                + "Default: 268435456 (256MB). Set to 536870912 for 512MB files.")
        .define(
            DUCKDB_THREADS,
            ConfigDef.Type.INT,
            0,
            ConfigDef.Importance.MEDIUM,
            "Number of threads for DuckDB to use. 0 means use all available processors. "
                + "Default: 0 (all cores)")
        .define(
            PARALLEL_PARTITION_FLUSH,
            ConfigDef.Type.BOOLEAN,
            true,
            ConfigDef.Importance.MEDIUM,
            "Enable parallel flushing of partitions for higher throughput. Default: true");
  }

  public DucklakeSinkConfig(ConfigDef definition, Map<?, ?> originals) {
    super(definition, originals);
  }

  public String getDucklakeCatalogUri() {
    return getPassword(DUCKLAKE_CATALOG_URI).value();
  }

  public String getS3UrlStyle() {
    return getString(S3_URL_STYLE);
  }

  public String getS3UseSsl() {
    return getString(S3_USE_SSL);
  }

  public String getS3Endpoint() {
    return getString(S3_ENDPOINT);
  }

  public String getS3AccessKeyId() {
    return getPassword(S3_ACCESS_KEY_ID).value();
  }

  public String getS3SecretAccessKey() {
    return getPassword(S3_SECRET_ACCESS_KEY).value();
  }

  public String getDataPath() {
    return getString(DATA_PATH);
  }

  public int getConsumerOverrideMaxPollRecords() {
    return getInt(CONSUMER_OVERRIDE_MAX_POLL_RECORDS);
  }

  /**
   * Returns the number of records to buffer before writing to DuckLake.
   *
   * @return flush size in number of records, default 1000000 (1M records)
   */
  public int getFlushSize() {
    return getInt(FLUSH_SIZE);
  }

  /**
   * Returns the maximum time to buffer records before forcing a flush.
   *
   * @return flush interval in milliseconds, default 60000 (60 seconds)
   */
  public long getFlushIntervalMs() {
    return getLong(FLUSH_INTERVAL_MS);
  }

  /**
   * Returns the target file size in bytes before flushing the buffer.
   *
   * @return file size in bytes, default 268435456 (256MB)
   */
  public long getFileSizeBytes() {
    return getLong(FILE_SIZE_BYTES);
  }

  /**
   * Returns the number of threads DuckDB should use for parallel processing.
   *
   * @return number of threads, 0 means use all available processors
   */
  public int getDuckDbThreads() {
    int threads = getInt(DUCKDB_THREADS);
    if (threads <= 0) {
      return Runtime.getRuntime().availableProcessors();
    }
    return threads;
  }

  /**
   * Returns whether parallel partition flushing is enabled.
   *
   * @return true if parallel partition flush is enabled, default true
   */
  public boolean isParallelPartitionFlushEnabled() {
    return getBoolean(PARALLEL_PARTITION_FLUSH);
  }

  /**
   * Default is 'off' (data inlining disabled). If value is the string "off" (case-insensitive),
   * data inlining is disabled and an empty OptionalInt is returned.
   */
  public OptionalInt getDataInliningRowLimit() {
    var raw = getString(DATA_INLINING_ROW_LIMIT);
    if (raw == null) {
      return OptionalInt.empty();
    }
    var trimmed = raw.trim();
    if (trimmed.isEmpty()) {
      return OptionalInt.empty();
    }
    if ("off".equalsIgnoreCase(trimmed)) {
      return OptionalInt.empty();
    }
    try {
      var v = Integer.parseInt(trimmed);
      if (v < 0) {
        throw new ConfigException(
            "" + DATA_INLINING_ROW_LIMIT + " must be a non-negative integer or 'off'");
      }
      return OptionalInt.of(v);
    } catch (NumberFormatException e) {
      throw new ConfigException("Invalid value for " + DATA_INLINING_ROW_LIMIT + ": " + raw);
    }
  }

  public Map<String, String> getTopicToTableMap() {
    try {
      return TopicToTableValidator.parseTopicToTableMap(getString(TOPICS_TABLES_MAP));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Gets the ID columns for a specific table.
   *
   * @param tableName the name of the table
   * @return array of ID column names, empty array if not configured
   */
  public String[] getTableIdColumns(String tableName) {
    String propertyKey = String.format(TABLE_ID_COLUMNS_PATTERN, tableName);
    Object raw = originals().get(propertyKey);
    if (raw == null) {
      return new String[0];
    }
    String value = raw.toString();
    if (value.trim().isEmpty()) {
      return new String[0];
    }
    return Arrays.stream(value.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .toArray(String[]::new);
  }

  /**
   * Gets the partition expressions for a specific table. These can be column names (e.g., "status",
   * "region") or function expressions (e.g., "year(created_at)", "month(created_at)",
   * "day(created_at)").
   *
   * @param tableName the name of the table
   * @return array of partition expressions, empty array if not configured
   */
  public String[] getTablePartitionByExpressions(String tableName) {
    String propertyKey = String.format(TABLE_PARTITION_BY_PATTERN, tableName);
    Object raw = originals().get(propertyKey);
    if (raw == null) {
      return new String[0];
    }
    String value = raw.toString();
    if (value.trim().isEmpty()) {
      return new String[0];
    }
    return Arrays.stream(value.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .toArray(String[]::new);
  }

  /**
   * Gets the auto-create setting for a specific table.
   *
   * @param tableName the name of the table
   * @return true if auto-create is enabled, false otherwise (default)
   */
  public boolean getTableAutoCreate(String tableName) {
    String propertyKey = String.format(TABLE_AUTO_CREATE_PATTERN, tableName);
    Object raw = originals().get(propertyKey);
    if (raw == null) {
      return false;
    }
    return Boolean.parseBoolean(raw.toString());
  }

  /**
   * Gets all table names that have been configured with table-specific properties.
   *
   * @return set of configured table names
   */
  public Set<String> getConfiguredTableNames() {
    Set<String> tableNames = new HashSet<>();

    // Extract table names from all table-specific properties
    for (String key : originals().keySet()) {
      if (key.startsWith("ducklake.table.")) {
        String[] parts = key.split("\\.");
        if (parts.length >= 3) {
          tableNames.add(parts[2]);
        }
      }
    }

    return tableNames;
  }

  /**
   * Checks if a table has any specific configuration.
   *
   * @param tableName the name of the table
   * @return true if the table has specific configuration
   */
  public boolean hasTableConfiguration(String tableName) {
    String idColumnsKey = String.format(TABLE_ID_COLUMNS_PATTERN, tableName);
    String partitionByKey = String.format(TABLE_PARTITION_BY_PATTERN, tableName);
    String autoCreateKey = String.format(TABLE_AUTO_CREATE_PATTERN, tableName);

    return originals().containsKey(idColumnsKey)
        || originals().containsKey(partitionByKey)
        || originals().containsKey(autoCreateKey);
  }

  /**
   * Returns a Map<String,String> suitable for posting as connector "config" payload. This converts
   * the originals() map values to strings.
   */
  public Map<String, String> getConnectorConfigMap() {
    Map<String, String> out = new java.util.HashMap<>();
    for (Map.Entry<?, ?> e : originals().entrySet()) {
      out.put(String.valueOf(e.getKey()), e.getValue() != null ? String.valueOf(e.getValue()) : "");
    }
    return out;
  }

  /** Validator for data path configuration (S3, GCS, or local file paths). */
  static class DataPathValidator implements ConfigDef.Validator {
    @Override
    public void ensureValid(String name, Object value) {
      if (value == null) {
        throw new ConfigException(name, null, "Data path is required");
      }
      String path = value.toString().trim();
      if (path.isEmpty()) {
        throw new ConfigException(name, value, "Data path cannot be empty");
      }

      boolean isValidPath =
          S3_PATH_PATTERN.matcher(path).matches()
              || GCS_PATH_PATTERN.matcher(path).matches()
              || LOCAL_PATH_PATTERN.matcher(path).matches();

      if (!isValidPath) {
        throw new ConfigException(
            name,
            value,
            "Invalid data path format. Expected one of: "
                + "s3://bucket-name/path, s3a://bucket-name/path, "
                + "gs://bucket-name/path, or file:///absolute/path");
      }
    }

    @Override
    public String toString() {
      return "Valid data path (s3://, s3a://, gs://, or file:///)";
    }
  }
}
