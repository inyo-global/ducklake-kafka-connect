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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
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
  private static EmbeddedKafkaConnect embeddedKafkaConnect;

  @Container
  public static final KafkaContainer kafkaContainer =
      new KafkaContainer("apache/kafka-native:4.0.0")
          .withNetwork(network)
          .withNetworkAliases("kafka");

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
    // Stop embedded Kafka Connect
    if (embeddedKafkaConnect != null) {
      embeddedKafkaConnect.stop();
    }

    // Testcontainers manages container lifecycle; close shared network
    if (network != null) {
      network.close();
    }
  }

  @Test
  void connector() {

    // Start embedded Kafka Connect
    embeddedKafkaConnect = new EmbeddedKafkaConnect(kafkaContainer.getBootstrapServers());
    embeddedKafkaConnect.start();

    // Generate unique names using UUID to avoid conflicts between test runs
    String testId = UUID.randomUUID().toString().replace("-", "_");
    String topicName = "orders_" + testId;
    String tableName = "orders_" + testId;

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

    // Produce a simple JSON record to Kafka using dynamic topic name
    var props = new Properties();
    props.put("bootstrap.servers", kafkaContainer.getBootstrapServers());
    props.put("key.serializer", StringSerializer.class.getName());
    props.put("value.serializer", StringSerializer.class.getName());

    try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
      producer.send(new ProducerRecord<>(topicName, "key-1", "{\"id\":1,\"customer\":\"alice\"}"));
      producer.flush();
    }

    // Create connector configuration and start it via embedded Kafka Connect
    var connectorConfig = getConnectorConfigMap(topicName, tableName);
    embeddedKafkaConnect.createConnector("ducklake-sink-" + testId, connectorConfig);
    embeddedKafkaConnect.ensureConnectorRunning("ducklake-sink-" + testId);

    // Wait for connector to be running properly first (fast check with short timeout)
    String connectorName = "ducklake-sink-" + testId;
    Awaitility.await("Connector to be running")
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(() -> assertConnectorRunning(connectorName));

    // Then wait for data to be processed and inserted (longer timeout for data processing)
    var factory = getDucklakeConnectionFactory(topicName, tableName);
    Awaitility.await("Data to be inserted into table")
        .atMost(Duration.ofSeconds(60))
        .pollInterval(Duration.ofSeconds(2))
        .ignoreExceptions() // Ignore table-not-found exceptions during async creation
        .untilAsserted(() -> assertTableDataExists(factory, tableName));
  }

  private static void assertConnectorRunning(String connectorName) {
    if (embeddedKafkaConnect == null) {
      throw new RuntimeException("EmbeddedKafkaConnect is not initialized");
    }

    try {
      var status = embeddedKafkaConnect.getConnectorStatus(connectorName);

      // Assert connector state is RUNNING
      String connectorState = status.connector().state();
      if ("FAILED".equals(connectorState)) {
        throw new RuntimeException("Connector failed: " + status.connector().trace());
      }
      assertEquals(
          "RUNNING",
          connectorState,
          "Connector should be in RUNNING state but was: " + connectorState);

      // Assert all tasks are RUNNING
      boolean anyTaskFailed =
          status.tasks().stream().anyMatch(task -> "FAILED".equals(task.state()));
      if (anyTaskFailed) {
        var failedTask =
            status.tasks().stream()
                .filter(task -> "FAILED".equals(task.state()))
                .findFirst()
                .orElse(null);
        if (failedTask != null) {
          throw new RuntimeException("Connector task failed: " + failedTask.trace());
        }
      }

      boolean allTasksRunning =
          status.tasks().stream().allMatch(task -> "RUNNING".equals(task.state()));
      assertTrue(
          allTasksRunning,
          "All connector tasks should be RUNNING. Current tasks states: "
              + status.tasks().stream().map(ConnectorStateInfo.AbstractState::state).toList());

    } catch (Exception e) {
      if (e instanceof RuntimeException) {
        throw e;
      }
      throw new RuntimeException("Failed to check connector status: " + e.getMessage(), e);
    }
  }

  private static void assertTableDataExists(DucklakeConnectionFactory factory, String tableName) {
    try {
      factory.create();
      try (var duckConn = factory.getConnection();
          var st = duckConn.createStatement();
          var rs = st.executeQuery("SELECT id, customer FROM lake.main." + tableName)) {

        assertTrue(rs.next(), "Expected at least one row in table " + tableName);

        int id = rs.getInt("id");
        String customer = rs.getString("customer");

        assertEquals(1, id, "Expected id to be 1");
        assertEquals("alice", customer, "Expected customer to be 'alice'");
      }
    } catch (SQLException e) {
      // Check if this is a "table doesn't exist" error (expected during async creation)
      if (e.getMessage().contains("does not exist")) {
        // This is expected - table is being created asynchronously
        throw new AssertionError("Table not ready yet: " + e.getMessage(), e);
      } else {
        // This is a real SQL error (connection, syntax, etc.) - fail immediately
        throw new RuntimeException("SQL configuration error: " + e.getMessage(), e);
      }
    } catch (Exception e) {
      // Connection or other configuration errors - fail immediately
      throw new RuntimeException("Configuration error during data assertion: " + e.getMessage(), e);
    } finally {
      factory.close();
    }
  }

  private static @NotNull Map<String, String> getConnectorConfigMap(
      String topicName, String tableName) {
    var connectorProps = new java.util.HashMap<String, String>();
    connectorProps.put("connector.class", "com.inyo.ducklake.connect.DucklakeSinkConnector");
    connectorProps.put("name", "ducklake-sink"); // Add the required name field
    connectorProps.put("tasks.max", "1");
    connectorProps.put("topics", topicName);

    // Use mapped ports since embedded Kafka Connect runs in the same JVM as the test
    String pgHost = postgres.getHost();
    Integer pgPort = postgres.getMappedPort(5432);
    connectorProps.put(
        "ducklake.catalog_uri",
        String.format(
            "postgres:dbname=ducklake_catalog host=%s port=%d user=duck password=duck",
            pgHost, pgPort));
    connectorProps.put("topic2table.map", topicName + ":" + tableName);
    connectorProps.put("ducklake.data_path", "s3://test-bucket/");
    connectorProps.put("ducklake.table." + tableName + ".id-columns", "id");
    connectorProps.put("ducklake.table." + tableName + ".auto-create", "true");
    connectorProps.put("s3.url_style", "path");
    connectorProps.put("s3.use_ssl", "false");

    // Use mapped ports for MinIO
    String minioHost = minio.getHost();
    Integer minioPort = minio.getMappedPort(9000);
    connectorProps.put("s3.endpoint", String.format("%s:%d", minioHost, minioPort));
    connectorProps.put("s3.access_key_id", "minio");
    connectorProps.put("s3.secret_access_key", "minio123");

    // Set low flush thresholds for integration tests
    connectorProps.put(DucklakeSinkConfig.FLUSH_SIZE, "1");
    connectorProps.put(DucklakeSinkConfig.FLUSH_INTERVAL_MS, "1000");
    connectorProps.put(DucklakeSinkConfig.FILE_SIZE_BYTES, "1024");

    return connectorProps;
  }

  private static @NotNull DucklakeConnectionFactory getDucklakeConnectionFactory(
      String topicName, String tableName) {
    var cfgMap = new java.util.HashMap<String, String>();
    // Test JVM uses localhost + mapped port to reach Postgres container
    String pgHost = postgres.getHost();
    Integer pgPort = postgres.getMappedPort(5432);
    cfgMap.put(
        DucklakeSinkConfig.DUCKLAKE_CATALOG_URI,
        String.format(
            "postgres:dbname=ducklake_catalog host=%s port=%d user=duck password=duck",
            pgHost, pgPort));

    // Required core properties for DucklakeSinkConfig
    cfgMap.put("topic2table.map", topicName + ":" + tableName);

    cfgMap.put("ducklake.data_path", "s3://test-bucket/");
    cfgMap.put("s3.url_style", "path");
    cfgMap.put("s3.use_ssl", "false");
    // Test JVM uses localhost + mapped port to reach MinIO container
    String minioHost = minio.getHost();
    Integer minioPort = minio.getMappedPort(9000);
    cfgMap.put("s3.endpoint", String.format("%s:%d", minioHost, minioPort));
    cfgMap.put("s3.access_key_id", "minio");
    cfgMap.put("s3.secret_access_key", "minio123");

    var dlConfig = new DucklakeSinkConfig(DucklakeSinkConfig.CONFIG_DEF, cfgMap);
    return new DucklakeConnectionFactory(dlConfig);
  }
}
