package com.pinax.kafka.redis.connect.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
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
    void taskConfigs_isASingleTaskWhateverIsAllowed() {
        Map<String, String> props = new HashMap<>();
        props.put(RedisSinkConfig.MASTER, "mymaster");
        props.put(RedisSinkConfig.MAILCHIMP_API_KEY, "key");
        props.put(RedisSinkConfig.TEMPLATE_SLUG, "slug");
        props.put("tasks.max", "1");
        connector.start(props);

        List<Map<String, String>> taskConfigs = connector.taskConfigs(3);

        assertEquals(List.of(props), taskConfigs);
    }

    @Test
    void start_moreThanOneTask_fails() {
        Map<String, String> props = new HashMap<>();
        props.put("tasks.max", "2");

        assertThrows(ConnectException.class, () -> connector.start(props));
    }
}
