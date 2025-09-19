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

import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@Testcontainers
class EndToEndIntegrationTest {

  private static final Network network = Network.newNetwork();

  @Container
  public static final KafkaContainer kafkaContainer =
      new KafkaContainer("apache/kafka-native:4.0.0")
          .withNetwork(network)
          .withNetworkAliases("kafka");

  @Container
  public static final KafkaConnectContainer kafkaConnectContainer =
      new KafkaConnectContainer()
          .withNetwork(network)
          .dependsOn(kafkaContainer)
          .withEnv("CONNECT_BOOTSTRAP_SERVERS", "kafka:9093")
          .withEnv("CONNECT_OFFSET_FLUSH_INTERVAL_MS", "500");

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
          // expose both S3 API (9000) and console (9001)
          .withExposedPorts(9000, 9001)
          .withCommand("server", "/data", "--console-address", ":9001");

  @AfterAll
  public static void tearDown() {
    // Testcontainers manages container lifecycle; close shared network
    if (network != null) {
      network.close();
    }
  }

  @Test
  void connector() {
    // Build MinIO client using host and mapped port so JVM can reach the container; use known
    // credentials
    var minioHost = minio.getHost();
    var minioPort = minio.getMappedPort(9000);
    var endpoint = String.format("http://%s:%d", minioHost, minioPort);
    try (var minioClient =
        MinioClient.builder().endpoint(endpoint).credentials("minio", "minio123").build()) {
      boolean exists =
          minioClient.bucketExists(
              io.minio.BucketExistsArgs.builder().bucket("test-bucket").build());
      if (!exists) {
        minioClient.makeBucket(MakeBucketArgs.builder().bucket("test-bucket").build());
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to ensure MinIO bucket 'test-bucket' exists", e);
    }

    // Produce a simple JSON record to Kafka
    var props = new Properties();
    props.put("bootstrap.servers", kafkaContainer.getBootstrapServers());
    props.put("key.serializer", StringSerializer.class.getName());
    props.put("value.serializer", StringSerializer.class.getName());

    try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
      producer.send(new ProducerRecord<>("orders", "key-1", "{\"id\":1,\"customer\":\"alice\"}"));
      producer.flush();
    }

    // Build connector configuration map and start it via Kafka Connect REST using
    // DucklakeSinkConfig
    var dlConfig = getDucklakeSinkConfig();
    kafkaConnectContainer.startConnector("ducklake-sink", dlConfig);
    kafkaConnectContainer.ensureConnectorRunning("ducklake-sink");

    // Wait until the destination table appears via Ducklake (DuckDB attaches the catalog)
    Awaitility.await()
        .untilAsserted(
            () -> {
              // Build a config map matching the connector settings so DucklakeConnectionFactory can
              // attach
              var factory = getDucklakeConnectionFactory();
              try {
                factory.create();
                try (var duckConn = factory.getConnection();
                    var st = duckConn.createStatement();
                    var rs = st.executeQuery("SELECT id, customer FROM lake.main.orders")) {
                  Assertions.assertTrue(rs.next(), "Expected at least one row in 'orders' table");

                  // Assert on the actual content
                  int id = rs.getInt("id");
                  String customer = rs.getString("customer");

                  Assertions.assertEquals(1, id, "Expected id to be 1");
                  Assertions.assertEquals("alice", customer, "Expected customer to be 'alice'");
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              } finally {
                factory.close();
              }
            });
  }

  private static @NotNull DucklakeSinkConfig getDucklakeSinkConfig() {
    var connectorProps = new java.util.HashMap<>();
    connectorProps.put("connector.class", "com.inyo.ducklake.connect.DucklakeSinkConnector");
    connectorProps.put("tasks.max", "1");
    connectorProps.put("topics", "orders");
    // Connector (running in Kafka Connect container) uses Docker network hostnames
    connectorProps.put(
        "ducklake.catalog_uri",
        "postgres:dbname=ducklake_catalog host=postgres user=duck password=duck");
    connectorProps.put("ducklake.tables", "orders");
    connectorProps.put("topic2table.map", "orders:orders");
    connectorProps.put("ducklake.data_path", "s3://test-bucket/");
    connectorProps.put("ducklake.table.orders.id-columns", "id");
    connectorProps.put("ducklake.table.orders.auto-create", "true");
    connectorProps.put("s3.url_style", "path");
    connectorProps.put("s3.use_ssl", "false");
    // Connector uses container hostname (minio:9000) accessible via Docker network
    connectorProps.put("s3.endpoint", "minio:9000");
    connectorProps.put("s3.access_key_id", "minio");
    connectorProps.put("s3._secret_access_key", "minio123");
    connectorProps.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
    connectorProps.put("key.converter.schemas.enable", "false");
    connectorProps.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
    connectorProps.put("value.converter.schemas.enable", "false");

    return new DucklakeSinkConfig(DucklakeSinkConfig.CONFIG_DEF, connectorProps);
  }

  private static @NotNull DucklakeConnectionFactory getDucklakeConnectionFactory() {
    var cfgMap = new java.util.HashMap<>();
    // Test JVM uses localhost + mapped port to reach Postgres container
    String pgHost = "localhost";
    Integer pgPort = postgres.getMappedPort(5432);
    cfgMap.put(
        DucklakeSinkConfig.DUCKLAKE_CATALOG_URI,
        String.format(
            "postgres:dbname=ducklake_catalog host=%s port=%d user=duck password=duck",
            pgHost, pgPort));

    // Required core properties for DucklakeSinkConfig
    cfgMap.put("ducklake.tables", "orders");
    cfgMap.put("topic2table.map", "orders:orders");

    cfgMap.put("ducklake.data_path", "s3://test-bucket/");
    cfgMap.put("s3.url_style", "path");
    cfgMap.put("s3.use_ssl", "false");
    // Test JVM uses localhost + mapped port to reach MinIO container
    String minioHost = "localhost";
    Integer minioPort = minio.getMappedPort(9000);
    cfgMap.put("s3.endpoint", String.format("%s:%d", minioHost, minioPort));
    cfgMap.put("s3.access_key_id", "minio");
    cfgMap.put("s3._secret_access_key", "minio123");

    var dlConfig = new DucklakeSinkConfig(DucklakeSinkConfig.CONFIG_DEF, cfgMap);
    return new DucklakeConnectionFactory(dlConfig);
  }
}
