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

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@Testcontainers
class AvroIntegrationTest {

  private static final Network network = Network.newNetwork();

  @Container
  public static final KafkaContainer kafkaContainer =
      new KafkaContainer("apache/kafka-native:4.0.0")
          .withNetwork(network)
          .withNetworkAliases("kafka")
          .withStartupTimeout(Duration.of(3, ChronoUnit.MINUTES));

  @Container
  @SuppressWarnings("resource")
  public static final PostgreSQLContainer<?> postgres =
      new PostgreSQLContainer<>("postgres:17")
          .withNetwork(network)
          .withNetworkAliases("postgres")
          .withDatabaseName("ducklake_catalog")
          .withUsername("duck")
          .withPassword("duck");

  @Container
  public static final MinIOContainer minio =
      new MinIOContainer("minio/minio:latest")
          .withNetwork(network)
          .withNetworkAliases("minio")
          .withEnv("MINIO_ROOT_USER", "minio")
          .withEnv("MINIO_ROOT_PASSWORD", "minio123")
          .withExposedPorts(9000)
          .withCommand("server", "/data");

  private static EmbeddedKafkaConnect embeddedKafkaConnect;

  @BeforeAll
  static void setUp() {
    embeddedKafkaConnect = new EmbeddedKafkaConnect(kafkaContainer.getBootstrapServers());
    embeddedKafkaConnect.start();
  }

  @AfterAll
  static void tearDown() {
    if (embeddedKafkaConnect != null) {
      embeddedKafkaConnect.stop();
    }
    if (network != null) {
      network.close();
    }
  }

  @Test
  void shouldProcessAvroMessagesEndToEnd() throws Exception {
    // Unique IDs per run to avoid residual state conflicts
    var testId = UUID.randomUUID().toString().replace("-", "_");
    var topicName = "avro_test_topic_" + testId;
    var tableName = "users_avro_" + testId;
    var connectorName = "avro-test-connector-" + testId;

    // Use mock schema registry for tests
    var schemaRegistryUrl = "mock://test-schema-registry";

    // Build Avro schema
    var userSchema =
        new Schema.Parser()
            .parse(
                """
                                        {
                                          "type": "record",
                                          "name": "User",
                                          "fields": [
                                            {"name": "name", "type": "string"},
                                            {"name": "age", "type": "int"}
                                          ]
                                        }
                                        """
                    .stripIndent());

    var record = new GenericRecordBuilder(userSchema).set("name", "Alice").set("age", 30).build();

    // Create Kafka producer that uses KafkaAvroSerializer for the value and mock schema registry
    var producerProps = new Properties();
    producerProps.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    producerProps.put("schema.registry.url", schemaRegistryUrl);

    // Ensure the topic exists before producing (prevents UNKNOWN_TOPIC_OR_PARTITION on some
    // brokers)
    var adminProps = new Properties();
    adminProps.put(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
    try (var admin = AdminClient.create(adminProps)) {
      var newTopic = new NewTopic(topicName, 1, (short) 1);
      try {
        admin.createTopics(Collections.singletonList(newTopic)).all().get(10, TimeUnit.SECONDS);
      } catch (Exception e) {
        // Ignore if topic already exists or creation failed transiently; producer will still
        // attempt
        System.out.println("Warning: topic creation returned error (ignored): " + e.getMessage());
      }
    }

    try (var producer = new KafkaProducer<String, Object>(producerProps)) {
      var pr = new ProducerRecord<String, Object>(topicName, "key-1", record);
      producer.send(pr).get();
      producer.flush();
    }

    // Create connector configuration for Avro converter
    var connectorConfig =
        getAvroConnectorConfig(connectorName, topicName, tableName, schemaRegistryUrl);

    // Ensure MinIO bucket exists (test-bucket) - same as Arrow test
    var minioHost = minio.getHost();
    var minioPort = minio.getMappedPort(9000);
    var endpoint = String.format("http://%s:%d", minioHost, minioPort);
    try (var minioClient =
        MinioClient.builder().endpoint(endpoint).credentials("minio", "minio123").build()) {
      var bucketExists =
          minioClient.bucketExists(
              io.minio.BucketExistsArgs.builder().bucket("test-bucket").build());
      if (!bucketExists) {
        minioClient.makeBucket(MakeBucketArgs.builder().bucket("test-bucket").build());
      }
    }

    embeddedKafkaConnect.createConnector(connectorName, connectorConfig);

    // Wait for connector fully running (connector + tasks)
    IntegrationTestUtils.awaitConnectorRunning(embeddedKafkaConnect, connectorName);

    // Assert data row using shared utility
    var factory = IntegrationTestUtils.connectionFactory(topicName, tableName, postgres, minio);
    Awaitility.await("Avro row ingested")
        .atMost(Duration.ofSeconds(60))
        .pollInterval(Duration.ofSeconds(2))
        .ignoreExceptions()
        .untilAsserted(
            () ->
                IntegrationTestUtils.assertTableRow(
                    factory,
                    tableName,
                    "SELECT name, age FROM lake.main." + tableName,
                    Map.of("name", "Alice", "age", 30)));

    System.out.println("✅ Avro integration test completed successfully (data verified)");
  }

  private static @NotNull Map<String, String> getAvroConnectorConfig(
      String connectorName, String topicName, String tableName, String schemaRegistryUrl) {
    var cfg =
        IntegrationTestUtils.baseConnectorConfig(
            topicName, tableName, postgres, minio, true, "name");
    cfg.put("name", connectorName);
    cfg.put("connector.class", "com.inyo.ducklake.connect.DucklakeSinkConnector");
    cfg.put("tasks.max", "1");
    cfg.put("topics", topicName);
    cfg.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
    cfg.put("value.converter", "io.confluent.connect.avro.AvroConverter");
    cfg.put("value.converter.schema.registry.url", schemaRegistryUrl);
    cfg.put("ducklake.batch.size", "1000");
    return cfg;
  }
}
