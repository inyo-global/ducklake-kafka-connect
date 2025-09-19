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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

public class DucklakeSinkConnector extends SinkConnector {

  /**
   * This class uses clickhouse-sink-connector as reference implementation Source: <a
   * href="https://github.com/databricks/iceberg-kafka-connect">iceberg-kafka-connect</a> Licensed
   * under the Apache License, Version 2.0
   */
  private Map<String, String> props;

  @Override
  public void start(Map<String, String> connectorProps) {
    this.props = new HashMap<>(connectorProps);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return DucklakeSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    return IntStream.range(0, maxTasks)
        .mapToObj(
            i -> {
              Map<String, String> map = new HashMap<>(props);
              map.put("task.id", String.valueOf(i));
              return map;
            })
        .collect(Collectors.toList());
  }

  @Override
  public void stop() {}

  @Override
  public Config validate(Map<String, String> connectorConfigs) {
    // The connector supports dynamic table-scoped properties like
    // ducklake.table.<table>.id-columns etc. Kafka's ConfigDef-based
    // validation treats unknown keys as errors, so filter out these
    // dynamic keys before delegating to the standard validation.
    Map<String, String> filtered = connectorConfigs.entrySet().stream()
        .filter(e -> !e.getKey().startsWith("ducklake.table."))
        .collect(java.util.stream.Collectors.toMap(java.util.Map.Entry::getKey, java.util.Map.Entry::getValue));

    return super.validate(filtered);
  }

  @Override
  public ConfigDef config() {
    return DucklakeSinkConfig.CONFIG_DEF;
  }

  @Override
  public String version() {
    return DucklakeSinkConfig.VERSION;
  }
}
