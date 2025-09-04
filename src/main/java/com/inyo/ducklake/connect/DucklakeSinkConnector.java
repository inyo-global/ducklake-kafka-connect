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
    return super.validate(connectorConfigs);
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
