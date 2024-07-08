package com.pinax.kafka.redis.connect.sink;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.ClusterPipeline;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;

public class RedisSinkTask extends SinkTask {
    private final DefaultJedisClientConfig DEFAULT_CLIENT_CONFIG = DefaultJedisClientConfig.builder().build();
    private final Logger logger = LoggerFactory.getLogger(RedisSinkConnector.class);

    private ClusterPipeline pipeline = null;

    private List<String> redisHosts;

    @Override
    public void start(Map<String, String> properties) {
        logger.info("Starting Redis sink task {}", properties);

        AbstractConfig config = new AbstractConfig(RedisSinkConfig.CONFIG_DEF, properties);

        // Prepare Redis connection
        redisHosts = config.getList(RedisSinkConfig.HOSTS);

        try {
            // Create a Redis cluster connection
            Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();

            for (String redisHostPort : redisHosts) {
                String[] parts = redisHostPort.split(":");
                if (parts.length != 2) {
                    throw new RuntimeException("Invalid Redis host and port: " + redisHostPort);
                }

                String redisHost = parts[0];
                int redisPort = Integer.parseInt(parts[1]);
                jedisClusterNodes.add(new HostAndPort(redisHost, redisPort));
            }

            pipeline = new ClusterPipeline(jedisClusterNodes, DEFAULT_CLIENT_CONFIG);
            logger.info("Redis connection created");
        } catch (Exception e) {
            logger.error("Failed to create Redis connection", e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.size() > 0) {
            logger.debug("Received records from Connect");
        }

        String key = null;
        String value = null;

        try {
            for (SinkRecord record : records) {
                logger.debug("Processing record: {}", record);
                key = record.key() == null ? "" : record.key().toString();
                value = record.value() == null ? "" : record.value().toString();

                // value:expireAt
                String[] parts = value.split(":");
                if (parts.length != 2) {
                    throw new DataException("Invalid value format: " + value);
                }

                double doubleValue = Double.parseDouble(parts[0]);
                long expireAtValue = Long.parseLong(parts[1]);

                pipeline.incrByFloat(key, doubleValue);
                pipeline.expireAt(key, expireAtValue);

                logger.debug("Record written to Redis: key={}, value={}", key, value);
            }

        } catch (Exception e) {
            final String message = "Failed to write record to Redis: key=" + key + ", value=" + value;
            logger.error(message, e);
            throw new DataException(message, e);
        }

        pipeline.sync();
    }

    @Override
    public void stop() {
        logger.info("Stopping Redis sink task");
        if (pipeline != null) {
            pipeline.close();
        }
    }

    @Override
    public String version() {
        return RedisSinkConnector.VERSION;
    }
}
