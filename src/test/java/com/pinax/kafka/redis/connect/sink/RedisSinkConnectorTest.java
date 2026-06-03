package com.pinax.kafka.redis.connect.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class RedisSinkConnectorTest {

    private final RedisSinkConnector connector = new RedisSinkConnector();

    @Test
    void version_isReported() {
        assertEquals(RedisSinkConnector.VERSION, connector.version());
    }

    @Test
    void config_returnsSharedConfigDef() {
        assertSame(RedisSinkConfig.CONFIG_DEF, connector.config());
    }

    @Test
    void taskClass_isRedisSinkTask() {
        assertEquals(RedisSinkTask.class, connector.taskClass());
    }

    @Test
    void taskConfigs_replicatesStartConfigForEachTask() {
        Map<String, String> props = new HashMap<>();
        props.put(RedisSinkConfig.MASTER, "mymaster");
        props.put(RedisSinkConfig.MAILCHIMP_API_KEY, "key");
        props.put(RedisSinkConfig.TEMPLATE_SLUG, "slug");
        connector.start(props);

        List<Map<String, String>> taskConfigs = connector.taskConfigs(3);

        assertEquals(3, taskConfigs.size());
        for (Map<String, String> taskConfig : taskConfigs) {
            assertEquals(props, taskConfig);
        }
    }
}
