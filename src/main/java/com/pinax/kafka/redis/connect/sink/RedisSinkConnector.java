package com.pinax.kafka.redis.connect.sink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisSinkConnector extends SinkConnector {

    protected static final String VERSION = "0.0.1"; // TODO: Get artifact version from pom.xml

    private final Logger logger = LoggerFactory.getLogger(RedisSinkConnector.class);

    private static final String TASKS_MAX = "tasks.max";

    private Map<String, String> configs = null;

    @Override
    public String version() {
        return VERSION;
    }

    @Override
    public void start(Map<String, String> configMap) {
        logger.info("Starting connector {}", configMap);

        // The offsets of all partitions of a topic are in one Redis hash that every
        // transaction WATCHes, so tasks running side by side would keep refusing each
        // other's transactions. Fail instead of running slowly.
        String tasksMax = configMap.get(TASKS_MAX);
        if (tasksMax != null && Integer.parseInt(tasksMax.trim()) > 1) {
            throw new ConnectException("This connector runs a single task, set " + TASKS_MAX + " to 1 (is "
                    + tasksMax.trim() + ")");
        }
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
        // Always a single task, see start().
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        taskConfigs.add(configs);
        return taskConfigs;
    }

    @Override
    public void stop() {
        logger.info("Stopping connector");
    }
}
