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

import static com.inyo.ducklake.connect.DucklakeSinkConfig.TOPICS_TABLES_MAP;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicToTableValidator implements ConfigDef.Validator {

  /**
   * This class uses clickhouse-sink-connector as reference implementation Source: <a
   * href="https://github.com/Altinity/clickhouse-sink-connector">clickhouse-sink-connector</a>
   * Licensed under the Apache License, Version 2.0
   */
  private static final Logger LOG = LoggerFactory.getLogger(TopicToTableValidator.class);

  /** Default constructor for the TopicToTableValidator class. */
  public TopicToTableValidator() {}

  /**
   * Ensures the validity of the topic-to-table mapping configuration.
   *
   * <p>This method is called by the framework when: 1. The connector is started. 2. The validate
   * REST API is called.
   *
   * <p>It checks if the provided value (topic-to-table map) is in the expected format. If the
   * format is invalid, a ConfigException is thrown.
   *
   * @param name the name of the configuration being validated.
   * @param value the value to be validated.
   * @throws ConfigException if the value does not match the expected format.
   */
  @Override
  public void ensureValid(String name, Object value) {
    String s = (String) value;

    // If the value is null or empty, it's considered valid
    if (s == null || s.isEmpty()) {
      return;
    }

    try {
      // Try to parse the topic-to-table map using the utility method
      parseTopicToTableMap(s);
    } catch (Exception e) {
      // Log the stack trace if an error occurs during validation
      throw new ConfigException(name, value, e.getMessage());
    }
  }

  /**
   * Returns a description of the expected format for the topic-to-table map.
   *
   * @return a string description of the expected format for the topic-to-table map.
   */
  public String toString() {
    return "Topic to table map format : comma-separated tuples, e.g."
        + " <topic-1>:<table-1>,<topic-2>:<table-2>,... ";
  }

  /**
   * Parses the topic-to-table configuration and returns it as a map.
   *
   * <p>This method processes the input string in the format:
   * <topic1>:<table1>,<topic2>:<table2>,... It splits the input string by commas and colons to form
   * key-value pairs, validating each entry along the way. If any invalid format is found, an
   * exception is thrown.
   *
   * <p>When no mappings are configured (null or empty input), this method returns an empty map. The
   * DucklakeWriterFactory will then use the topic name itself as the table name for any unmapped
   * topics via the getOrDefault(topic, topic) pattern.
   *
   * @param input a comma-separated list of topic-to-table mappings, or null/empty for no mappings.
   * @return a map where the keys are topics and the values are corresponding tables. Returns empty
   *     map when no mappings are configured, allowing topics to map to themselves.
   * @throws Exception if the format of the input is invalid.
   */
  @Nonnull
  public static Map<String, String> parseTopicToTableMap(String input) throws Exception {
    Map<String, String> topic2Table = new HashMap<>();

    // Handle null or empty input by returning empty map
    if (input == null || input.trim().isEmpty()) {
      return topic2Table;
    }

    boolean isInvalid = false;
    for (String str : input.split(",")) {
      // Skip empty strings that result from splitting empty input
      if (str.trim().isEmpty()) {
        continue;
      }

      String[] tt = str.split(":");

      if (tt.length != 2 || tt[0].trim().isEmpty() || tt[1].trim().isEmpty()) {
        LOG.error("Invalid {} config format: {}", TOPICS_TABLES_MAP, input);
        isInvalid = true;
        continue;
      }

      String topic = tt[0].trim();
      String table = tt[1].trim();

      if (!isValidTable(table)) {
        LOG.error(
            "table name {} should have at least 1 character, start with _a-zA-Z, "
                + "and only contains _a-zA-z0-9- (hyphens allowed)",
            table);
        isInvalid = true;
      }

      if (topic2Table.containsKey(topic)) {
        LOG.error("topic name {} is duplicated", topic);
        isInvalid = true;
      }

      if (topic2Table.containsValue(table)) {
        LOG.error("table name {} is duplicated", table);
        isInvalid = true;
      }
      topic2Table.put(tt[0].trim(), tt[1].trim());
    }
    if (isInvalid) {
      throw new Exception("Invalid table");
    }
    return topic2Table;
  }

  private static boolean isValidTable(String table) {
    // Allow hyphens in table names since DuckDB supports them when quoted
    // Updated pattern to include hyphens: [a-zA-Z_][a-zA-Z0-9_-]*
    return table.matches("^[a-zA-Z_][a-zA-Z0-9_-]*$");
  }
}
