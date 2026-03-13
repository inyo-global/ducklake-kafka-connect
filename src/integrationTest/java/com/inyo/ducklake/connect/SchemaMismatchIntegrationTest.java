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

import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Reproduces the VectorAppender type mismatch bug that occurs when records in separate poll cycles
 * have different types for the same field (e.g., a field that is sometimes a string, sometimes a
 * number). Before the fix, consolidateBatches would crash the task. After the fix,
 * BatchConsolidator groups contiguous compatible batches and writes incompatible batches as separate
 * runs.
 */
@Testcontainers
class SchemaMismatchIntegrationTest {

  private static final Network network = Network.newNetwork();
  private static EmbeddedKafkaConnect embeddedKafkaConnect;

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

  /**
   * Sends records where the same field has different JSON types across separate poll cycles. With
   * flush.size > number of records, records from separate polls produce separate Arrow batches with
   * different schemas. When the time-based flush triggers, BatchConsolidator groups them into
   * contiguous runs by compatible schema and writes each run separately. Before the fix,
   * VectorBatchAppender would throw on the type mismatch.
   */
  @Test
  void schemaMismatchDuringConsolidation() throws Exception {
    embeddedKafkaConnect = new EmbeddedKafkaConnect(kafkaContainer.getBootstrapServers());
    embeddedKafkaConnect.start();

    String testId = UUID.randomUUID().toString().replace("-", "_");
    String topicName = "schema_mismatch_" + testId;
    String tableName = "schema_mismatch_" + testId;

    // Create MinIO bucket
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
    }

    // Configure and start connector FIRST so it's actively polling when records arrive.
    // This ensures records from separate sends end up in separate poll cycles (separate batches).
    var connectorConfig = new java.util.HashMap<String, String>();
    connectorConfig.put("connector.class", "com.inyo.ducklake.connect.DucklakeSinkConnector");
    connectorConfig.put("name", "ducklake-sink-" + testId);
    connectorConfig.put("tasks.max", "1");
    connectorConfig.put("topics", topicName);
    String pgHost = postgres.getHost();
    Integer pgPort = postgres.getMappedPort(5432);
    connectorConfig.put(
        "ducklake.catalog_uri",
        String.format(
            "postgres:dbname=ducklake_catalog host=%s port=%d user=duck password=duck",
            pgHost, pgPort));
    connectorConfig.put("topic2table.map", topicName + ":" + tableName);
    connectorConfig.put("ducklake.data_path", "s3://test-bucket/");
    connectorConfig.put("ducklake.table." + tableName + ".auto-create", "true");
    connectorConfig.put("s3.url_style", "path");
    connectorConfig.put("s3.use_ssl", "false");
    String minioHostCfg = minio.getHost();
    Integer minioPortCfg = minio.getMappedPort(9000);
    connectorConfig.put("s3.endpoint", String.format("%s:%d", minioHostCfg, minioPortCfg));
    connectorConfig.put("s3.access_key_id", "minio");
    connectorConfig.put("s3.secret_access_key", "minio123");

    // High flush.size so count-based flush won't trigger;
    // 8s flush interval so time-based flush triggers after both records are buffered
    connectorConfig.put(DucklakeSinkConfig.FLUSH_SIZE, "1000");
    connectorConfig.put(DucklakeSinkConfig.FLUSH_INTERVAL_MS, "8000");
    connectorConfig.put(DucklakeSinkConfig.FILE_SIZE_BYTES, "268435456");

    String connectorName = "ducklake-sink-" + testId;
    embeddedKafkaConnect.createConnector(connectorName, connectorConfig);
    embeddedKafkaConnect.ensureConnectorRunning(connectorName);

    // Wait for connector to be actively polling
    Thread.sleep(3000);

    var props = new Properties();
    props.put("bootstrap.servers", kafkaContainer.getBootstrapServers());
    props.put("key.serializer", StringSerializer.class.getName());
    props.put("value.serializer", StringSerializer.class.getName());

    // Send first record: "value" is a string
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
      producer.send(new ProducerRecord<>(topicName, "key-1", "{\"id\":1,\"value\":\"hello\"}"));
      producer.flush();
      System.out.println("Sent first record (value=string)");
    }

    // Wait for the first record to be consumed in its own poll cycle (creating a VarChar batch)
    Thread.sleep(3000);

    // Send second record: "value" is a number (different Arrow type)
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
      producer.send(new ProducerRecord<>(topicName, "key-2", "{\"id\":2,\"value\":42}"));
      producer.flush();
      System.out.println("Sent second record (value=number)");
    }

    // Wait for the time-based flush to trigger (8s interval) and complete.
    // The flush will attempt to consolidate the two batches with mismatched schemas.
    // Before the fix: task crashes with VectorBatchAppender type mismatch.
    // After the fix: BatchConsolidator writes them as separate runs and task stays RUNNING.
    Thread.sleep(15000);

    // Verify the task is still running
    var status = embeddedKafkaConnect.getConnectorStatus(connectorName);
    assertEquals("RUNNING", status.connector().state(), "Connector should still be RUNNING");

    for (ConnectorStateInfo.TaskState task : status.tasks()) {
      if ("FAILED".equals(task.state())) {
        System.err.println("Task " + task.id() + " FAILED with trace:\n" + task.trace());
      }
      assertEquals(
          "RUNNING",
          task.state(),
          "Task " + task.id() + " should be RUNNING but was FAILED. Trace: " + task.trace());
    }
  }
}
