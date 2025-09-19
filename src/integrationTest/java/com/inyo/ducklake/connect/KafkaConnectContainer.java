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

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.classic.methods.HttpGet;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.classic.methods.HttpPost;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.impl.classic.HttpClients;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.http.HttpStatus;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.http.io.entity.StringEntity;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.shaded.com.google.common.collect.Lists;
import org.testcontainers.shaded.com.google.common.collect.Maps;
import org.testcontainers.shaded.org.awaitility.Awaitility;

public class KafkaConnectContainer extends GenericContainer<KafkaConnectContainer> {

    private final static ObjectMapper MAPPER = new ObjectMapper();

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
        this.withEnv("KAFKA_OPTS", "--add-opens java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED");

        this.withFileSystemBind(localPlugin, KC_PLUGIN_DIR, BindMode.READ_WRITE);

        this.setWaitStrategy(
                new HttpWaitStrategy()
                        .forPath("/connectors")
                        .forPort(PORT)
                        .withStartupTimeout(Duration.ofSeconds(STARTUP_TIMEOUT_IN_SECONDS)));
    }

    @Override
    protected void containerIsStarted(InspectContainerResponse containerInfo, boolean reused) {
        super.containerIsStarted(containerInfo, reused);
    }

    public void startConnector(String connectorName, DucklakeSinkConfig config) {
        try (var client = HttpClients.createDefault()) {
            var request = new HttpPost(String.format("http://localhost:%d/connectors", getMappedPort(PORT)));
            // Build payload: { "name": "...", "config": { ... } }
            Map<String, Object> payload = Maps.newHashMap();
            payload.put("name", connectorName);
            payload.put("config", config.getConnectorConfigMap());
            String body = MAPPER.writeValueAsString(payload);
            request.setHeader("Content-Type", "application/json");
            request.setEntity(new StringEntity(body));
            client.execute(request, response -> null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void ensureConnectorRunning(String name) {
        try (var client = HttpClients.createDefault()) {
            var port = getMappedPort(PORT);
            var request = new HttpGet(String.format("http://localhost:%d/connectors/%s/status", port, name));
            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .until(() -> client.execute(request, response -> {
                        if (response.getCode() == HttpStatus.SC_OK) {
                            JsonNode root = MAPPER.readTree(response.getEntity().getContent());
                            String connectorState = root.get("connector").get("state").asText();
                            var taskNodes = (ArrayNode) root.get("tasks");
                            var taskStates = Lists.newArrayList();
                            taskNodes.forEach(node -> taskStates.add(node.get("state").asText()));
                            return "RUNNING".equals(connectorState)
                                    && taskStates.stream().allMatch("RUNNING"::equals);
                        }
                        return false;
                    }));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
