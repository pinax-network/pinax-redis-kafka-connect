package com.pinax.kafka.redis.connect.sink;

import org.apache.kafka.common.config.ConfigDef;

public class RedisSinkConfig {

        // Redis sink configuration
        public static final String MASTER = "master";
        public static final String HOSTS = "hosts";
        public static final String OFFSETS_KEY = "offsetsKey";
        public static final String RETRY_BACKOFF_MS = "retryBackoffMs";
        public static final String CONFLICT_RETRY_BACKOFF_MS = "conflictRetryBackoffMs";
        public static final String MAX_RETRIES = "maxRetries";

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
                        .define(OFFSETS_KEY,
                                        ConfigDef.Type.STRING,
                                        "kafka-connect:redis-sink:offsets",
                                        ConfigDef.Importance.MEDIUM,
                                        "Prefix of the Redis hashes (one per topic, named <offsetsKey>:<topic>) "
                                                        + "that hold the offset of the last record applied "
                                                        + "per partition")
                        .define(RETRY_BACKOFF_MS,
                                        ConfigDef.Type.LONG,
                                        5000L,
                                        // Connect ignores a timeout of 0.
                                        ConfigDef.Range.atLeast(1L),
                                        ConfigDef.Importance.LOW,
                                        "Time in milliseconds before a batch is retried after Redis was "
                                                        + "unreachable or not accepting writes")
                        .define(CONFLICT_RETRY_BACKOFF_MS,
                                        ConfigDef.Type.LONG,
                                        1000L,
                                        ConfigDef.Range.atLeast(1L),
                                        ConfigDef.Importance.LOW,
                                        "Time in milliseconds before a batch is retried after Redis refused "
                                                        + "its transaction because the stored offsets were "
                                                        + "changed by another task")
                        .define(MAX_RETRIES,
                                        ConfigDef.Type.INT,
                                        12,
                                        ConfigDef.Range.atLeast(0),
                                        ConfigDef.Importance.LOW,
                                        "How often in a row a batch is retried before the task fails. With "
                                                        + "the default backoff that is about a minute of Redis "
                                                        + "being unreachable")
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
