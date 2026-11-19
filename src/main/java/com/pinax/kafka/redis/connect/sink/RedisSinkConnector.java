package com.pinax.kafka.redis.connect.sink;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisSinkConnector extends SinkConnector {

    // The version in pom.xml, written into this resource when the jar is built.
    protected static final String VERSION = readVersion();

    private final Logger logger = LoggerFactory.getLogger(RedisSinkConnector.class);

    private static final String TASKS_MAX = "tasks.max";

    private Map<String, String> configs = null;

    @Override
    public String version() {
        return VERSION;
    }

    @Override
    public void start(Map<String, String> configMap) {
        logger.info("Starting connector {}", RedisSinkConfig.withoutSecrets(configMap));

        // A config stored before validate() rejected it. Not failed here: Connect
        // would keep running the tasks it created earlier. taskConfigs() brings them
        // down to one.
        if (moreThanOneTask(configMap)) {
            logger.warn("{} is {}, but this connector runs a single task; set it to 1",
                    TASKS_MAX, configMap.get(TASKS_MAX));
        }
        // Defensive copy so the connector does not retain a reference to a map
        // owned by the caller (and which the caller could later mutate).
        configs = new HashMap<>(configMap);
    }

    /**
     * Rejects a config that asks for more than one task. The offsets of all
     * partitions of a topic are in one Redis hash that every transaction WATCHes, so
     * tasks running side by side would keep refusing each other's transactions.
     */
    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        Config config = super.validate(connectorConfigs);
        if (!moreThanOneTask(connectorConfigs)) {
            return config;
        }

        String tasksMax = connectorConfigs.get(TASKS_MAX);
        ConfigValue value = new ConfigValue(TASKS_MAX, tasksMax, new ArrayList<>(), new ArrayList<>());
        value.addErrorMessage("This connector runs a single task, set " + TASKS_MAX + " to 1");
        List<ConfigValue> values = new ArrayList<>(config.configValues());
        values.add(value);
        return new Config(values);
    }

    private static boolean moreThanOneTask(Map<String, String> connectorConfigs) {
        String tasksMax = connectorConfigs.get(TASKS_MAX);
        try {
            return tasksMax != null && Integer.parseInt(tasksMax.trim()) > 1;
        } catch (NumberFormatException e) {
            // Not a number: Connect reports that itself.
            return false;
        }
    }

    private static String readVersion() {
        try (InputStream in = RedisSinkConnector.class
                .getResourceAsStream("/pinax-redis-kafka-connect-version.properties")) {
            if (in != null) {
                Properties properties = new Properties();
                properties.load(in);
                return properties.getProperty("version", "unknown");
            }
        } catch (IOException e) {
            // Reported as unknown below.
        }
        return "unknown";
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
        // Always a single task, see validate().
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        taskConfigs.add(configs);
        return taskConfigs;
    }

    @Override
    public void stop() {
        logger.info("Stopping connector");
    }
}
