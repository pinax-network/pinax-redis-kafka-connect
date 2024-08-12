package com.pinax.kafka.redis.connect.sink;

import org.apache.kafka.common.config.ConfigDef;

public class RedisSinkConfig {
        public static final String HOST = "host";

        public static final ConfigDef CONFIG_DEF = new ConfigDef()
                        .define(HOST,
                                        ConfigDef.Type.STRING,
                                        "localhost:6379",
                                        new RedisSinkConfigValidator(),
                                        ConfigDef.Importance.HIGH,
                                        "Redis host");
}
