package com.inyo.ducklake.connect;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

public class DucklakeSinkTask extends SinkTask {
    private static final System.Logger LOG = System.getLogger(String.valueOf(DucklakeSinkTask.class));
    private DucklakeSinkConfig config;
    private DucklakeConnectionFactory connectionFactory;

    @Override
    public String version() {
        return DucklakeSinkConfig.VERSION;
    }

    @Override
    public void start(Map<String, String> map) {
        this.config = new DucklakeSinkConfig(DucklakeSinkConfig.CONFIG_DEF, map);
        this.connectionFactory = new DucklakeConnectionFactory(config);
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        super.open(partitions);
        try {
            this.connectionFactory.create();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> collection) {

    }

    @Override
    public void stop() {
        try {
            this.connectionFactory.getConnection().close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
