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

import java.time.Duration;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

public class KafkaConnectContainer extends GenericContainer<KafkaConnectContainer> {

  private static final int PORT = 8083;
  private static final int STARTUP_TIMEOUT_IN_SECONDS = 30;
  public static final String KC_PLUGIN_DIR = "/test/kafka-connect";

  public KafkaConnectContainer() {
    super("confluentinc/cp-kafka-connect:8.0.0");
    var localPlugin = System.getProperty("distribution.path");

    this.withExposedPorts(PORT);
    this.withEnv("CONNECT_GROUP_ID", "kc");
    this.withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "kc_config");
    this.withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1");
    this.withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "kc_offsets");
    this.withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1");
    this.withEnv("CONNECT_STATUS_STORAGE_TOPIC", "kc_status");
    this.withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1");
    this.withEnv("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter");
    this.withEnv("CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE", "false");
    this.withEnv("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter");
    this.withEnv("CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE", "false");
    this.withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", "localhost");
    this.withEnv("CONNECT_PLUGIN_PATH", KC_PLUGIN_DIR);
    this.withEnv("CONNECT_LOG4J_LOGGERS", "org.apache.kafka.connect.runtime.isolation=DEBUG");

    this.withFileSystemBind(localPlugin, KC_PLUGIN_DIR, BindMode.READ_WRITE);

    this.setWaitStrategy(
        new HttpWaitStrategy()
            .forPath("/connectors")
            .forPort(PORT)
            .withStartupTimeout(Duration.ofSeconds(STARTUP_TIMEOUT_IN_SECONDS)));
  }
}
