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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.connect.connector.policy.AllConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.util.FutureCallback;
import org.testcontainers.shaded.org.awaitility.Awaitility;

public class EmbeddedKafkaConnect {

  private Connect<StandaloneHerder> connect;
  private StandaloneHerder herder;
  private OffsetBackingStore offsetBackingStore;
  private final Map<String, String> baseConfig;
  private final Duration startupTimeout;
  private File tempOffsetFile;

  public EmbeddedKafkaConnect(String bootstrapServers) {
    this(bootstrapServers, Duration.ofSeconds(30));
  }

  public EmbeddedKafkaConnect(String bootstrapServers, Duration startupTimeout) {
    this.startupTimeout = startupTimeout;
    this.baseConfig = createBaseConfig(bootstrapServers);
  }

  private Map<String, String> createBaseConfig(String bootstrapServers) {
    Map<String, String> config = new HashMap<>();

    // Create unique temporary offset file per instance to avoid conflicts
    try {
      String uniqueId = System.currentTimeMillis() + "-" + Thread.currentThread().getId();
      tempOffsetFile = Files.createTempFile("connect-offsets-" + uniqueId, ".tmp").toFile();
      tempOffsetFile.deleteOnExit();
      System.out.println("📁 Created unique offset file: " + tempOffsetFile.getAbsolutePath());
    } catch (IOException e) {
      throw new RuntimeException("Failed to create temporary offset file", e);
    }

    // Basic Kafka Connect configuration
    config.put(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    // Use StringConverter for keys since we're sending simple strings like "key-1"
    config.put(
        WorkerConfig.KEY_CONVERTER_CLASS_CONFIG,
        "org.apache.kafka.connect.storage.StringConverter");
    config.put(
        WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
    // Only disable schemas for JSON converter (value), not needed for StringConverter
    config.put("value.converter.schemas.enable", "false");

    // Consumer group configuration to avoid coordination issues
    String uniqueGroupId =
        "embedded-connect-" + System.currentTimeMillis() + "-" + Thread.currentThread().getId();
    config.put("consumer.group.id", uniqueGroupId);

    // Consumer timeout configurations to handle rebalancing better
    config.put("consumer.session.timeout.ms", "30000");
    config.put("consumer.heartbeat.interval.ms", "3000");
    config.put("consumer.max.poll.interval.ms", "300000");
    config.put("consumer.request.timeout.ms", "40000");
    config.put("consumer.retry.backoff.ms", "1000");

    // Additional consumer configurations for stability
    config.put("consumer.auto.offset.reset", "earliest");
    config.put("consumer.enable.auto.commit", "true");
    config.put("consumer.auto.commit.interval.ms", "5000");

    // Standalone mode configuration
    config.put(
        StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, tempOffsetFile.getAbsolutePath());

    // Plugin path - use the correct distribution path
    String distributionPath = System.getProperty("distribution.path");
    if (distributionPath != null && new File(distributionPath + "/lib").exists()) {
      config.put(WorkerConfig.PLUGIN_PATH_CONFIG, distributionPath + "/lib");
      System.out.println("Using distribution plugin path: " + distributionPath + "/lib");
    } else {
      // Multiple fallback strategies for different execution environments
      String currentDir = System.getProperty("user.dir");
      String[] possiblePaths = {
        // Gradle execution path
        currentDir + "/build/install/ducklake-kafka-connect/lib",
        // IntelliJ execution from project root
        currentDir + "/../../../build/install/ducklake-kafka-connect/lib",
        // Alternative IntelliJ paths
        currentDir + "/../../build/install/ducklake-kafka-connect/lib",
        currentDir + "/../../../../build/install/ducklake-kafka-connect/lib"
      };

      boolean foundValidPath = false;
      for (String path : possiblePaths) {
        File pluginDir = new File(path);
        if (pluginDir.exists() && pluginDir.isDirectory()) {
          config.put(WorkerConfig.PLUGIN_PATH_CONFIG, path);
          System.out.println("Using fallback plugin path: " + path);
          foundValidPath = true;
          break;
        }
      }

      if (!foundValidPath) {
        System.err.println("Warning: No valid plugin directory found. Tried paths:");
        for (String path : possiblePaths) {
          System.err.println("  - " + path);
        }
        System.err.println("Current working directory: " + currentDir);
        System.err.println("Kafka Connect will use classpath-based plugin loading");
        // Don't set plugin.path if directory doesn't exist - let Kafka Connect use classpath
      }
    }

    // Additional configuration for embedded usage
    config.put(
        WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG,
        AllConnectorClientConfigOverridePolicy.class.getName());

    return config;
  }

  public void start() {
    try {
      System.out.println("🔧 Starting Embedded Kafka Connect...");

      // Create standalone configuration
      StandaloneConfig config = new StandaloneConfig(baseConfig);

      System.out.println("📋 Kafka Connect configuration:");
      baseConfig.forEach((key, value) -> System.out.println("  " + key + " = " + value));

      // Initialize plugins
      Plugins plugins = new Plugins(baseConfig);
      plugins.compareAndSwapWithDelegatingLoader();

      // Create offset backing store with proper converter and unique file
      var converter = new JsonConverter();
      converter.configure(config.originalsWithPrefix(""), false);

      // Use FileOffsetBackingStore with unique file per instance to avoid conflicts
      offsetBackingStore = new FileOffsetBackingStore(converter);
      offsetBackingStore.configure(config);
      offsetBackingStore.start();

      System.out.println(
          "✅ Using FileOffsetBackingStore with unique file: " + tempOffsetFile.getName());

      // Create worker with proper parameters
      Worker worker =
          new Worker(
              "duck-embedded-connect",
              org.apache.kafka.common.utils.Time.SYSTEM,
              plugins,
              config,
              offsetBackingStore,
              new AllConnectorClientConfigOverridePolicy());

      // Create herder directly without Connect wrapper
      herder =
          new StandaloneHerder(
              worker, "kafka-cluster-id", new AllConnectorClientConfigOverridePolicy());

      // Start herder directly - no need for Connect wrapper
      herder.start();

      System.out.println("✅ Embedded Kafka Connect started successfully");

      // Wait for startup
      Thread.sleep(startupTimeout.toMillis());

    } catch (Exception e) {
      System.err.println("❌ Failed to start embedded Kafka Connect: " + e.getMessage());
      throw new RuntimeException("Failed to start embedded Kafka Connect", e);
    }
  }

  public void stop() {
    System.out.println("🛑 Stopping Embedded Kafka Connect...");

    try {
      if (herder != null) {
        System.out.println("Stopping herder...");
        herder.stop();
        herder = null;
      }

      if (offsetBackingStore != null) {
        System.out.println("Stopping offset backing store...");
        try {
          offsetBackingStore.stop();
        } catch (Exception e) {
          System.err.println("Error stopping offset backing store: " + e.getMessage());
        }
        offsetBackingStore = null;
      }

      if (connect != null) {
        System.out.println("Stopping connect...");
        connect.stop();
        connect = null;
      }

      // Clean up offset file
      if (tempOffsetFile != null && tempOffsetFile.exists()) {
        System.out.println("Deleting offset file: " + tempOffsetFile.getAbsolutePath());
        boolean deleted = tempOffsetFile.delete();
        if (!deleted) {
          System.err.println("Failed to delete offset file: " + tempOffsetFile.getAbsolutePath());
        }
        tempOffsetFile = null;
      }

      System.out.println("✅ Embedded Kafka Connect stopped successfully");

    } catch (Exception e) {
      System.err.println("❌ Error during Kafka Connect shutdown: " + e.getMessage());
    }
  }

  public void createConnector(String name, Map<String, String> config) {
    if (herder == null) {
      throw new IllegalStateException("Kafka Connect not started");
    }

    System.out.println("Creating connector '" + name + "' with config:");
    config.forEach((key, value) -> System.out.println("  " + key + " = " + value));

    var callback = new FutureCallback<Herder.Created<ConnectorInfo>>();
    herder.putConnectorConfig(name, config, false, callback);

    try {
      var result = callback.get(10, TimeUnit.SECONDS);
      System.out.println("Connector '" + name + "' created successfully: " + result);
    } catch (Exception e) {
      System.err.println("Failed to create connector '" + name + "': " + e.getMessage());
      throw new RuntimeException("Failed to create connector: " + name, e);
    }
  }

  public void ensureConnectorRunning(String name) {
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(Duration.ofSeconds(1))
        .until(
            () -> {
              try {
                ConnectorStateInfo status = herder.connectorStatus(name);

                String connectorState = status.connector().state();
                boolean connectorRunning = "RUNNING".equals(connectorState);

                // Log connector status
                System.out.println("Connector '" + name + "' state: " + connectorState);
                if (status.connector().trace() != null) {
                  System.out.println("Connector trace: " + status.connector().trace());
                }

                // Log task status
                if (status.tasks().isEmpty()) {
                  System.out.println("No tasks found for connector '" + name + "'");
                } else {
                  for (int i = 0; i < status.tasks().size(); i++) {
                    var task = status.tasks().get(i);
                    String taskState = task.state();
                    System.out.println("Task " + i + " state: " + taskState);
                    if (task.trace() != null) {
                      System.out.println("Task " + i + " trace: " + task.trace());
                    }
                  }
                }

                boolean allTasksRunning =
                    status.tasks().stream().allMatch(task -> "RUNNING".equals(task.state()));

                boolean isFullyRunning = connectorRunning && allTasksRunning;
                System.out.println(
                    "Connector fully running: "
                        + isFullyRunning
                        + " (connector: "
                        + connectorRunning
                        + ", tasks: "
                        + allTasksRunning
                        + ")");

                return isFullyRunning;
              } catch (Exception e) {
                System.err.println("Error checking connector status: " + e.getMessage());
                return false;
              }
            });
  }

  public ConnectorStateInfo getConnectorStatus(String name) {
    if (herder == null) {
      throw new IllegalStateException("Kafka Connect not started");
    }
    return herder.connectorStatus(name);
  }
}
