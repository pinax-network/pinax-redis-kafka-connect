package com.pinax.kafka.redis.connect.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

class RedisSinkConfigTest {

    private static Map<String, String> minimalProps() {
        Map<String, String> props = new HashMap<>();
        props.put(RedisSinkConfig.MAILCHIMP_API_KEY, "mailchimp-key");
        props.put(RedisSinkConfig.TEMPLATE_SLUG, "usage-template");
        return props;
    }

    @Test
    void appliesDefaultsForOptionalFields() {
        AbstractConfig config = new AbstractConfig(RedisSinkConfig.CONFIG_DEF, minimalProps());

        assertEquals("mymaster", config.getString(RedisSinkConfig.MASTER));
        assertEquals("info@pinax.network", config.getString(RedisSinkConfig.FROM));
        assertEquals(java.util.Arrays.asList("localhost:6379"),
                config.getList(RedisSinkConfig.HOSTS));
    }

    @Test
    void parsesProvidedValues() {
        Map<String, String> props = minimalProps();
        props.put(RedisSinkConfig.MASTER, "redis-master");
        props.put(RedisSinkConfig.HOSTS, "sentinel-a:26379,sentinel-b:26379");

        AbstractConfig config = new AbstractConfig(RedisSinkConfig.CONFIG_DEF, props);

        assertEquals("redis-master", config.getString(RedisSinkConfig.MASTER));
        assertEquals(java.util.Arrays.asList("sentinel-a:26379", "sentinel-b:26379"),
                config.getList(RedisSinkConfig.HOSTS));
    }

    @Test
    void mailchimpApiKey_isRequired() {
        Map<String, String> props = minimalProps();
        props.remove(RedisSinkConfig.MAILCHIMP_API_KEY);

        assertThrows(ConfigException.class,
                () -> new AbstractConfig(RedisSinkConfig.CONFIG_DEF, props));
    }

    @Test
    void templateSlug_isRequired() {
        Map<String, String> props = minimalProps();
        props.remove(RedisSinkConfig.TEMPLATE_SLUG);

        assertThrows(ConfigException.class,
                () -> new AbstractConfig(RedisSinkConfig.CONFIG_DEF, props));
    }

    @Test
    void invalidHostsFormat_isRejectedByValidator() {
        Map<String, String> props = minimalProps();
        props.put(RedisSinkConfig.HOSTS, "no-port-here");

        assertThrows(ConfigException.class,
                () -> new AbstractConfig(RedisSinkConfig.CONFIG_DEF, props));
    }
}
