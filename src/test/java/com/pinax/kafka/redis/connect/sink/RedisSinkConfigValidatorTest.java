package com.pinax.kafka.redis.connect.sink;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.List;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

class RedisSinkConfigValidatorTest {

    private final RedisSinkConfigValidator validator = new RedisSinkConfigValidator();

    @Test
    void hosts_validHostPortPairs_pass() {
        List<String> hosts = Arrays.asList("localhost:26379", "10.0.0.1:26380");
        assertDoesNotThrow(() -> validator.ensureValid(RedisSinkConfig.HOSTS, hosts));
    }

    @Test
    void hosts_missingPort_throws() {
        List<String> hosts = Arrays.asList("localhost");
        assertThrows(ConfigException.class,
                () -> validator.ensureValid(RedisSinkConfig.HOSTS, hosts));
    }

    @Test
    void hosts_nonNumericPort_throws() {
        List<String> hosts = Arrays.asList("localhost:abc");
        assertThrows(ConfigException.class,
                () -> validator.ensureValid(RedisSinkConfig.HOSTS, hosts));
    }

    @Test
    void from_validEmail_passes() {
        assertDoesNotThrow(() -> validator.ensureValid(RedisSinkConfig.FROM, "info@pinax.network"));
    }

    @Test
    void from_invalidEmail_throws() {
        assertThrows(ConfigException.class,
                () -> validator.ensureValid(RedisSinkConfig.FROM, "not-an-email"));
    }

    @Test
    void unrelatedKey_isIgnored() {
        // The validator only constrains HOSTS and FROM; anything else is a no-op.
        assertDoesNotThrow(() -> validator.ensureValid("some.other.key", "whatever"));
    }
}
