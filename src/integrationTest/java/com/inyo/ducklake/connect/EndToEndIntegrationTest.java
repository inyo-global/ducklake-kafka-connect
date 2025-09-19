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

import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;

class EndToEndIntegrationTest {

    private static Network network;
    private static KafkaContainer kafkaContainer;
    private static KafkaConnectContainer kafkaConnectContainer;
    private static PostgreSQLContainer<?> postgres;

    @BeforeAll
    public static void setUp() {
        network = Network.newNetwork();

        kafkaContainer =
                new KafkaContainer("apache/kafka-native:4.0.0")
                        .withNetwork(network)
                        .withNetworkAliases("kafka");
        kafkaContainer.start();

        kafkaConnectContainer = new KafkaConnectContainer();
        kafkaConnectContainer
                .withNetwork(network)
                .dependsOn(kafkaContainer)
                .withEnv("CONNECT_BOOTSTRAP_SERVERS", "kafka:9093")
                .withEnv("CONNECT_OFFSET_FLUSH_INTERVAL_MS", "500");

        postgres = new PostgreSQLContainer<>("postgres:17")
                .withNetwork(network);

        Startables.deepStart(Stream.of(kafkaContainer, kafkaConnectContainer)).join();
    }

    @AfterAll
    public static void tearDown() {
        if (kafkaContainer != null) {
            kafkaContainer.stop();
        }
        if (kafkaConnectContainer != null) {
            kafkaConnectContainer.stop();
        }
        if (network != null) {
            network.close();
        }

        if (postgres != null) {
            postgres.stop();
        }
    }

    @Test
    void connector() {
        Assertions.assertTrue(true);
    }
}
