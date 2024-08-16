package com.pinax.kafka.redis.connect.sink;

import org.apache.kafka.common.config.ConfigDef;

public class RedisSinkConfig {
        public static final String MASTER = "master";
        public static final String HOSTS = "hosts";

        public static final ConfigDef CONFIG_DEF = new ConfigDef()
                        .define(MASTER,
                                        ConfigDef.Type.STRING,
                                        "mymaster",
                                        ConfigDef.Importance.HIGH,
                                        "Redis master name")
                        .define(HOSTS,
                                        ConfigDef.Type.LIST,
                                        "localhost:6379",
                                        new RedisSinkConfigValidator(),
                                        ConfigDef.Importance.HIGH,
                                        "Redis hosts");
}
