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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
class ArrowIpcIntegrationTest {

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
          .withExposedPorts(9000)
          .withCommand("server", "/data");

  private static BufferAllocator allocator;
  private static ArrowIpcConverter arrowIpcConverter;

  @BeforeAll
  static void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
    arrowIpcConverter = new ArrowIpcConverter();

    var converterConfigs = new HashMap<String, Object>();
    arrowIpcConverter.configure(converterConfigs, false);

    embeddedKafkaConnect = new EmbeddedKafkaConnect(kafkaContainer.getBootstrapServers());
    embeddedKafkaConnect.start();
  }

  @AfterAll
  static void tearDown() {
    if (embeddedKafkaConnect != null) {
      embeddedKafkaConnect.stop();
    }
    if (arrowIpcConverter != null) {
      try {
        arrowIpcConverter.close();
      } catch (Exception e) {
        System.err.println("Warning: Failed to close ArrowIpcConverter: " + e.getMessage());
      }
    }
    if (allocator != null) {
      try {
        allocator.close();
      } catch (Exception e) {
        System.err.println("Warning: Failed to close allocator: " + e.getMessage());
      }
    }
  }

  @Test
  void shouldProcessArrowIpcMessagesEndToEnd() throws Exception {
    var testId = UUID.randomUUID().toString().replace("-", "_");
    var topicName = "arrow_ipc_test_topic_" + testId;
    var tableName = "arrow_ipc_tbl_" + testId;
    var connectorName = "arrow-ipc-test-connector-" + testId;

    // 1. Create Arrow IPC test data
    var arrowIpcData = createTestArrowIpcData();
    assertNotNull(arrowIpcData);
    assertTrue(arrowIpcData.length > 0);

    // 2. Verify the Arrow IPC data can be deserialized by our converter
    var schemaAndValue = arrowIpcConverter.toConnectData(topicName, arrowIpcData);
    assertNotNull(schemaAndValue);
    assertNotNull(schemaAndValue.schema());
    assertNotNull(schemaAndValue.value());
    assertInstanceOf(VectorSchemaRoot.class, schemaAndValue.value());

    System.out.println("✅ Arrow IPC data created and validated successfully");
    System.out.println("Schema: " + schemaAndValue.schema());
    System.out.println("Rows: " + ((VectorSchemaRoot) schemaAndValue.value()).getRowCount());

    // 3. Create Kafka producer with Arrow IPC converter
    var producerProps = new Properties();
    producerProps.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

    try (var producer = new KafkaProducer<String, byte[]>(producerProps)) {
      // 4. Send Arrow IPC data to Kafka topic
      var record = new ProducerRecord<>(topicName, "test-key", arrowIpcData);
      producer.send(record).get();
      producer.flush();

      System.out.println("✅ Arrow IPC data sent to Kafka topic: " + topicName);
    }

    // Ensure MinIO bucket exists (test-bucket)
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

    // 5. Create connector configuration for Arrow IPC
    var connectorConfig = createArrowIpcConnectorConfig(connectorName, topicName, tableName);

    // 6. Deploy connector
    embeddedKafkaConnect.createConnector(connectorName, connectorConfig);

    // 7. Wait for connector/tasks running
    IntegrationTestUtils.awaitConnectorRunning(embeddedKafkaConnect, connectorName);

    System.out.println("✅ Arrow IPC connector deployed and running");

    // 8. Assert ingested data
    var factory = IntegrationTestUtils.connectionFactory(topicName, tableName, postgres, minio);
    // Build expected row assertion after ingestion (id=1001, name, age)
    Awaitility.await("Arrow IPC row ingested")
        .atMost(Duration.ofSeconds(60))
        .pollInterval(Duration.ofSeconds(2))
        .ignoreExceptions()
        .untilAsserted(
            () ->
                IntegrationTestUtils.assertTableRow(
                    factory,
                    tableName,
                    "SELECT id, name, age FROM lake.main." + tableName,
                    Map.of("id", 1001, "name", "TestUser", "age", 42)));

    System.out.println("✅ Arrow IPC integration test completed successfully (data verified)!");
  }

  @Test
  void shouldHandleMultipleArrowIpcRecords() throws Exception {
    var topicName = "arrow-ipc-multi-test-topic";

    // Create multiple Arrow IPC records with different data
    var record1 = createTestArrowIpcData("Alice", 25);
    var record2 = createTestArrowIpcData("Bob", 30);
    var record3 = createTestArrowIpcData("Charlie", 35);

    // Verify all records can be processed
    for (int i = 0; i < 3; i++) {
      var record = i == 0 ? record1 : i == 1 ? record2 : record3;
      var result = arrowIpcConverter.toConnectData(topicName, record);

      assertNotNull(result);
      assertNotNull(result.value());
      assertInstanceOf(VectorSchemaRoot.class, result.value());

      var root = (VectorSchemaRoot) result.value();
      assertEquals(1, root.getRowCount());
    }

    System.out.println("✅ Multiple Arrow IPC records test completed successfully!");
  }

  @Test
  void shouldValidateArrowIpcConverterPerformance() throws Exception {
    var topicName = "arrow-ipc-perf-test-topic";
    var recordCount = 100;

    var startTime = System.currentTimeMillis();

    // Test conversion performance
    for (int i = 0; i < recordCount; i++) {
      var arrowData = createTestArrowIpcData("User" + i, 20 + i);
      var result = arrowIpcConverter.toConnectData(topicName, arrowData);

      assertNotNull(result);
      assertNotNull(result.value());
    }

    var endTime = System.currentTimeMillis();
    var duration = endTime - startTime;

    System.out.println("✅ Processed " + recordCount + " Arrow IPC records in " + duration + "ms");
    System.out.println("Average time per record: " + (duration / (double) recordCount) + "ms");

    // Should process at least 10 records per second
    assertTrue(duration < recordCount * 100, "Performance test failed - too slow");
  }

  private byte[] createTestArrowIpcData() throws Exception {
    return createTestArrowIpcData("TestUser", 42);
  }

  private byte[] createTestArrowIpcData(String name, int age) throws Exception {
    // Create Arrow schema
    var fields =
        Arrays.asList(
            new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
            new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
            new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null));
    var schema = new Schema(fields);

    // Create VectorSchemaRoot with test data
    try (var root = VectorSchemaRoot.create(schema, allocator)) {
      root.allocateNew();

      var idVector = (IntVector) root.getVector("id");
      var nameVector = (VarCharVector) root.getVector("name");
      var ageVector = (IntVector) root.getVector("age");

      idVector.setSafe(0, 1001);
      nameVector.setSafe(0, name.getBytes());
      ageVector.setSafe(0, age);

      root.setRowCount(1);

      // Convert to Arrow IPC bytes
      return vectorSchemaRootToBytes(root);
    }
  }

  private byte[] vectorSchemaRootToBytes(VectorSchemaRoot root) throws Exception {
    var outputStream = new ByteArrayOutputStream();
    var channel = Channels.newChannel(outputStream);

    try (var writer = new ArrowStreamWriter(root, null, channel)) {
      writer.start();
      if (root.getRowCount() > 0) {
        writer.writeBatch();
      }
      writer.end();
    }

    return outputStream.toByteArray();
  }

  private Map<String, String> createArrowIpcConnectorConfig(
      String connectorName, String topicName, String tableName) {
    var config =
        IntegrationTestUtils.baseConnectorConfig(topicName, tableName, postgres, minio, true, "id");
    config.put("name", connectorName);
    config.put("connector.class", "com.inyo.ducklake.connect.DucklakeSinkConnector");
    config.put("tasks.max", "1");
    config.put("topics", topicName);
    config.put("value.converter", "com.inyo.ducklake.connect.ArrowIpcConverter");
    config.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
    config.put("ducklake.batch.size", "1000");
    return config;
  }
}
