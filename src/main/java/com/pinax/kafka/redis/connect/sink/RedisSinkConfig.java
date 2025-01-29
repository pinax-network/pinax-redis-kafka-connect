package com.pinax.kafka.redis.connect.sink;

import org.apache.kafka.common.config.ConfigDef;

public class RedisSinkConfig {

        // Redis sink configuration
        public static final String MASTER = "master";
        public static final String HOSTS = "hosts";

        // Email sink configuration
        public static final String FROM = "from";
        public static final String HOST = "host";
        public static final String PORT = "port";
        public static final String USERNAME = "username";
        public static final String PASSWORD = "password";

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
                                        "Redis hosts")
                        .define(FROM,
                                        ConfigDef.Type.STRING,
                                        "info@pinax.network", // TODO: change to correct email
                                        new RedisSinkConfigValidator(),
                                        ConfigDef.Importance.HIGH,
                                        "Email that will be used to send usage notification")
                        .define(HOST,
                                        ConfigDef.Type.STRING,
                                        "smtp.gmail.com", // TODO: change to correct host
                                        ConfigDef.Importance.HIGH,
                                        "SMTP host")
                        .define(PORT,
                                        ConfigDef.Type.INT,
                                        587,
                                        ConfigDef.Importance.HIGH,
                                        "SMTP port")
                        .define(USERNAME,
                                        ConfigDef.Type.STRING,
                                        "username",
                                        ConfigDef.Importance.HIGH,
                                        "SMTP username")
                        .define(PASSWORD,
                                        ConfigDef.Type.STRING,
                                        "password",
                                        ConfigDef.Importance.HIGH,
                                        "SMTP password");
}
