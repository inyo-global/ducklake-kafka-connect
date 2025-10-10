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
import java.util.Set;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class DucklakeSinkConfig extends AbstractConfig {

  /** This class used iceberg-kafka-connect as reference implementation */
  public static final String VERSION = "1.0.0";

  public static final String DUCKLAKE_CATALOG_URI = "ducklake.catalog_uri";

  public static final ConfigDef CONFIG_DEF = newConfigDef();

  static final String DATA_PATH = "ducklake.data_path";
  static final String TOPICS_TABLES_MAP = "topic2table.map";
  static final String S3_URL_STYLE = "s3.url_style";
  static final String S3_USE_SSL = "s3.use_ssl";
  static final String S3_ENDPOINT = "s3.endpoint";
  static final String S3_ACCESS_KEY_ID = "s3.access_key_id";
  static final String S3_SECRET_ACCESS_KEY = "s3.secret_access_key";
  static final String CONSUMER_OVERRIDE_MAX_POLL_RECORDS = "consumer.override.max.poll.records";

  // Table-specific configuration property patterns
  static final String TABLE_ID_COLUMNS_PATTERN = "ducklake.table.%s.id-columns";
  static final String TABLE_PARTITION_BY_PATTERN = "ducklake.table.%s.partition-by";
  static final String TABLE_AUTO_CREATE_PATTERN = "ducklake.table.%s.auto-create";

  private static ConfigDef newConfigDef() {
    return new ConfigDef()
        .define(
            DUCKLAKE_CATALOG_URI,
            ConfigDef.Type.STRING,
            ConfigDef.Importance.HIGH,
            "Ducklake catalog URI, e.g., postgres:dbname=ducklake_catalog host=localhost")
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
            ConfigDef.Type.STRING,
            ConfigDef.Importance.HIGH,
            "The ID of the key to use")
        .define(
            S3_SECRET_ACCESS_KEY,
            ConfigDef.Type.STRING,
            ConfigDef.Importance.HIGH,
            "The secret of the key to use")
        .define(
            CONSUMER_OVERRIDE_MAX_POLL_RECORDS,
            ConfigDef.Type.INT,
            1024,
            ConfigDef.Importance.MEDIUM,
            "Maximum number of records returned in a single call to poll(). Defaults to 1024 if not specified.");
  }

  public DucklakeSinkConfig(ConfigDef definition, Map<?, ?> originals) {
    super(definition, originals);
  }

  public String getDucklakeCatalogUri() {
    return getString(DUCKLAKE_CATALOG_URI);
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
    return getString(S3_ACCESS_KEY_ID);
  }

  public String getS3SecretAccessKey() {
    return getString(S3_SECRET_ACCESS_KEY);
  }

  public String getDataPath() {
    return getString(DATA_PATH);
  }

  public int getConsumerOverrideMaxPollRecords() {
    return getInt(CONSUMER_OVERRIDE_MAX_POLL_RECORDS);
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
}
