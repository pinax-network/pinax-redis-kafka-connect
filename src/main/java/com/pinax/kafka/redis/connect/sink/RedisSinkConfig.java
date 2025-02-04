package com.pinax.kafka.redis.connect.sink;

import org.apache.kafka.common.config.ConfigDef;

public class RedisSinkConfig {

        // Redis sink configuration
        public static final String MASTER = "master";
        public static final String HOSTS = "hosts";

        // Email sink configuration
        public static final String FROM = "from";
        public static final String MAILCHIMP_API_KEY = "mailchimpApiKey";
        public static final String TEMPLATE_SLUG = "templateSlug";

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
                                        "Address used to send usage notification emails")
                        .define(MAILCHIMP_API_KEY,
                                        ConfigDef.Type.STRING,
                                        ConfigDef.Importance.HIGH,
                                        "Mailchimp API key")
                        .define(TEMPLATE_SLUG,
                                        ConfigDef.Type.STRING,
                                        ConfigDef.Importance.HIGH,
                                        "Mailchimp template slug");
}
