package com.pinax.kafka.redis.connect.sink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisSinkConnector extends SinkConnector {

    protected static final String VERSION = "0.0.1"; // TODO: Get artifact version from pom.xml

    private final Logger logger = LoggerFactory.getLogger(RedisSinkConnector.class);

    private Map<String, String> configs = null;

    @Override
    public String version() {
        return VERSION;
    }

    @Override
    public void start(Map<String, String> configMap) {
        logger.info("Starting connector {}", configMap);
        // Defensive copy so the connector does not retain a reference to a map
        // owned by the caller (and which the caller could later mutate).
        configs = new HashMap<>(configMap);
    }

    @Override
    public ConfigDef config() {
        return RedisSinkConfig.CONFIG_DEF;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return RedisSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxConfigs) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        for (int task = 0; task < maxConfigs; task++) {
            taskConfigs.add(configs);
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
        logger.info("Stopping connector");
    }
}
