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

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@Testcontainers
class ProdLikeConfigIntegrationTest {

  private static final Network network = Network.newNetwork();
  private static EmbeddedKafkaConnect embeddedKafkaConnect;

  // removed KafkaContainer; we'll use a prod bootstrap instead

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
          .withExposedPorts(9000, 9001)
          .withCommand("server", "/data", "--console-address", ":9001");

  @AfterAll
  public static void tearDown() {
    if (embeddedKafkaConnect != null) {
      embeddedKafkaConnect.stop();
    }
    if (network != null) {
      network.close();
    }
  }

  @Test
  void shouldRunConnectorWithProdLikeConfig() {
    // Resolve prod bootstrap (allow override via -Dprod.kafka.bootstrap)
    var bootstrapServers =
        System.getProperty(
            "prod.kafka.bootstrap", "my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092");

    // Start embedded Kafka Connect pointing to prod Kafka
    embeddedKafkaConnect = new EmbeddedKafkaConnect(bootstrapServers);
    embeddedKafkaConnect.start();

    // Ensure MinIO bucket exists
    var minioHost = minio.getHost();
    var minioPort = minio.getMappedPort(9000);
    var endpoint = String.format("http://%s:%d", minioHost, minioPort);
    try (var minioClient =
        MinioClient.builder().endpoint(endpoint).credentials("minio", "minio123").build()) {
      var bucket = "test-bucket";
      var exists =
          minioClient.bucketExists(io.minio.BucketExistsArgs.builder().bucket(bucket).build());
      if (!exists) {
        minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucket).build());
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to ensure MinIO bucket exists", e);
    }

    // Pre-create non-auto-created table: batch_transaction_finished
    var factory = connectionFactory();
    try {
      factory.create();
      try (var conn = factory.getConnection();
          var st = conn.createStatement()) {
        st.execute(
            "CREATE TABLE IF NOT EXISTS lake.main.batch_transaction_finished ("
                + "address JSON, "
                + "agentId VARCHAR, "
                + "amount VARCHAR, "
                + "batchId VARCHAR, "
                + "cancellationDisclosure VARCHAR, "
                + "client JSON, "
                + "contactInfo VARCHAR, "
                + "createdAt VARCHAR, "
                + "externalId VARCHAR, "
                + "id VARCHAR, "
                + "payment JSON, "
                + "product VARCHAR, "
                + "receipt VARCHAR, "
                + "rightToRefund VARCHAR, "
                + "status VARCHAR, "
                + "tenantId VARCHAR, "
                + "validationErrors JSON)");
      }
    } catch (SQLException e) {
      throw new RuntimeException("Failed to pre-create table batch_transaction_finished", e);
    } finally {
      factory.close();
    }

    // Build connector config as provided + MinIO & catalog
    var connectorConfig = connectorConfig();

    var connectorName = "ducklake-prod-like-connector";
    embeddedKafkaConnect.createConnector(connectorName, connectorConfig);

    // Wait connector RUNNING and keep it stable for a bit
    Awaitility.await()
        .atMost(Duration.ofSeconds(60))
        .pollInterval(Duration.ofSeconds(2))
        .until(
            () -> {
              var state = embeddedKafkaConnect.getConnectorStatus(connectorName);
              return state != null && "RUNNING".equals(state.connector().state());
            });

    // Run indefinitely; log connector and task states periodically
    for (; ; ) {
      try {
        var status = embeddedKafkaConnect.getConnectorStatus(connectorName);
        System.out.println(
            "[keepalive] Connector '" + connectorName + "' state: " + status.connector().state());
        if (!status.tasks().isEmpty()) {
          for (int i = 0; i < status.tasks().size(); i++) {
            var t = status.tasks().get(i);
            System.out.println(
                "[keepalive] Task "
                    + i
                    + " state: "
                    + t.state()
                    + (t.trace() != null ? ", trace=" + t.trace() : ""));
          }
        }
        Thread.sleep(60_000);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        break; // exit if interrupted explicitly
      } catch (Exception e) {
        System.err.println("[keepalive] Error while checking connector status: " + e.getMessage());
        try {
          Thread.sleep(30_000);
        } catch (InterruptedException ie2) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
  }

  private static void assertTableCountAtLeast(
      DucklakeConnectionFactory factory, String tableName, int expectedMin) throws Exception {
    factory.create();
    try (var conn = factory.getConnection();
        var st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT COUNT(*) as c FROM lake.main." + tableName)) {
      assertTrue(rs.next(), "Expected a COUNT(*) row for table " + tableName);
      var count = rs.getInt("c");
      assertTrue(
          count >= expectedMin, "Expected at least " + expectedMin + " rows in " + tableName);
    }
  }

  private static @NotNull Map<String, String> connectorConfig() {
    var config = new HashMap<String, String>();

    // Base connector
    config.put("name", "ducklake-prod-like-connector");
    config.put("connector.class", "com.inyo.ducklake.connect.DucklakeSinkConnector");
    config.put("tasks.max", "1");

    // Converters
    config.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
    config.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
    config.put("value.converter.schemas.enable", "false");

    // Topics and mapping
    config.put("topics", "batchTransactionProcessedEvents");
    config.put("topic2table.map", "batchTransactionProcessedEvents:batch_transaction_finished");

    // Table configs
    config.put("ducklake.table.batch_transaction_finished.auto-create", "false");
    config.put("ducklake.table.batch_transaction_finished.partition-by", "createdAt");

    // Transforms & predicates
    config.put("transforms", "insertTS,TimestampConverter");
    config.put("predicates", "isSpecificTopic");
    config.put(
        "predicates.isSpecificTopic.type",
        "org.apache.kafka.connect.transforms.predicates.TopicNameMatches");
    config.put("predicates.isSpecificTopic.pattern", "batchTransactionProcessedEvents");
    config.put("transforms.insertTS.type", "org.apache.kafka.connect.transforms.InsertField$Value");
    config.put("transforms.insertTS.timestamp.field", "createdAt");
    config.put(
        "transforms.TimestampConverter.type",
        "org.apache.kafka.connect.transforms.TimestampConverter$Value");
    config.put("transforms.TimestampConverter.field", "createdAt");
    config.put("transforms.TimestampConverter.format", "yyyy-MM-dd");
    config.put("transforms.TimestampConverter.target.type", "string");

    // Consumer overrides
    config.put("consumer.override.max.poll.records", "2000");
    config.put("consumer.override.max.partition.fetch.bytes", "102400");

    // S3/MinIO + Ducklake
    var pgHost = postgres.getHost();
    var pgPort = postgres.getMappedPort(5432);
    config.put(
        "ducklake.catalog_uri",
        String.format(
            "postgres:dbname=ducklake_catalog host=%s port=%d user=duck password=duck",
            pgHost, pgPort));

    config.put("ducklake.data_path", "s3://test-bucket/");

    config.put("s3.url_style", "path");
    config.put("s3.use_ssl", "false");
    var minioHost = minio.getHost();
    var minioPort = minio.getMappedPort(9000);
    config.put("s3.endpoint", String.format("%s:%d", minioHost, minioPort));
    config.put("s3.access_key_id", "minio");
    config.put("s3.secret_access_key", "minio123");
    config.put("s3.region", "us-east-1");

    return config;
  }

  private static @NotNull DucklakeConnectionFactory connectionFactory() {
    var cfgMap = new HashMap<String, String>();

    var pgHost = postgres.getHost();
    var pgPort = postgres.getMappedPort(5432);
    cfgMap.put(
        DucklakeSinkConfig.DUCKLAKE_CATALOG_URI,
        String.format(
            "postgres:dbname=ducklake_catalog host=%s port=%d user=duck password=duck",
            pgHost, pgPort));

    cfgMap.put(
        "topic2table.map",
        "batchTransaction-inputs:batch_transaction_input,batchTransactionProcessedEvents:batch_transaction_finished");

    cfgMap.put("ducklake.data_path", "s3://test-bucket/");
    cfgMap.put("s3.url_style", "path");
    cfgMap.put("s3.use_ssl", "false");
    var minioHost = minio.getHost();
    var minioPort = minio.getMappedPort(9000);
    cfgMap.put("s3.endpoint", String.format("%s:%d", minioHost, minioPort));
    cfgMap.put("s3.access_key_id", "minio");
    cfgMap.put("s3.secret_access_key", "minio123");

    var dlConfig = new DucklakeSinkConfig(DucklakeSinkConfig.CONFIG_DEF, cfgMap);
    return new DucklakeConnectionFactory(dlConfig);
  }
}
