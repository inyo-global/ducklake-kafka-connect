package com.inyo.ducklake.connect;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.HashMap;
import java.util.Map;

import static com.inyo.ducklake.connect.DucklakeSinkConfig.TOPICS_TABLES_MAP;

public class TopicToTableValidator implements ConfigDef.Validator {

    /**
     * This class uses clickhouse-sink-connector as reference implementation
     * Source: <a href="https://github.com/Altinity/clickhouse-sink-connector">clickhouse-sink-connector</a>
     * Licensed under the Apache License, Version 2.0
     */

    private static final System.Logger LOG = System.getLogger(String.valueOf(TopicToTableValidator.class));

    /**
     * Default constructor for the TopicToTableValidator class.
     */
    public TopicToTableValidator() {
    }

    /**
     * Ensures the validity of the topic-to-table mapping configuration.
     * <p>
     * This method is called by the framework when:
     * 1. The connector is started.
     * 2. The validate REST API is called.
     * </p>
     * <p>
     * It checks if the provided value (topic-to-table map) is in the expected format.
     * If the format is invalid, a ConfigException is thrown.
     * </p>
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
            if (parseTopicToTableMap(s) == null) {
                throw new ConfigException(name, value,
                        "Format: <topic-1>:<table-1>,<topic-2>:<table-2>,...");
            }
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
     * <p>
     * This method processes the input string in the format: <topic1>:<table1>,<topic2>:<table2>,...
     * It splits the input string by commas and colons to form key-value pairs, validating
     * each entry along the way. If any invalid format is found, an exception is thrown.
     * </p>
     *
     * @param input a comma-separated list of topic-to-table mappings.
     * @return a map where the keys are topics and the values are corresponding tables.
     * @throws Exception if the format of the input is invalid.
     */
    public static Map<String, String> parseTopicToTableMap(String input) throws Exception {
        Map<String, String> topic2Table = new HashMap<>();
        boolean isInvalid = false;
        for (String str : input.split(",")) {
            String[] tt = str.split(":");

            if (tt.length != 2 || tt[0].trim().isEmpty() || tt[1].trim().isEmpty()) {
                LOG.log(System.Logger.Level.ERROR, "Invalid {} config format: {}", TOPICS_TABLES_MAP, input);
                return null;
            }

            String topic = tt[0].trim();
            String table = tt[1].trim();

            if (!isValidTable(table)) {
                LOG.log(System.Logger.Level.ERROR,
                                "table name {} should have at least 2 "
                                        + "characters, start with _a-zA-Z, and only contains "
                                        + "_$a-zA-z0-9",
                                table);
                isInvalid = true;
            }

            if (topic2Table.containsKey(topic)) {
                LOG.log(System.Logger.Level.ERROR,"topic name {} is duplicated", topic);
                isInvalid = true;
            }

            if (topic2Table.containsValue(table)) {
                LOG.log(System.Logger.Level.ERROR,"table name {} is duplicated", table);
                isInvalid = true;
            }
            topic2Table.put(tt[0].trim(), tt[1].trim());
        }
        if (isInvalid) {
            throw new Exception("Invalidap table");
        }
        return topic2Table;
    }

    private static boolean isValidTable(String table) {
        return table.matches("^[a-zA-Z_][a-zA-Z0-9_]+$");
    }
}