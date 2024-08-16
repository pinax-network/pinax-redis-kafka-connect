package com.pinax.kafka.redis.connect.sink;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.Pipeline;

public class RedisSinkTask extends SinkTask {
    private final Logger logger = LoggerFactory.getLogger(RedisSinkConnector.class);

    private Set<String> sentinels = null;

    private Jedis jedis = null;
    private JedisSentinelPool jedisSentinelPool = null;
    private Pipeline jedisPipeline = null;

    private List<String> redisHosts;
    private String redisMaster;

    @Override
    public void start(Map<String, String> properties) {
        logger.info("Starting Redis sink task {}", properties);

        AbstractConfig config = new AbstractConfig(RedisSinkConfig.CONFIG_DEF, properties);

        // Prepare Redis connection
        redisHosts = config.getList(RedisSinkConfig.HOSTS);
        redisMaster = config.getString(RedisSinkConfig.MASTER);

        try {

            for (String redisHostPort : redisHosts) {
                sentinels.add(redisHostPort);
            }

            jedisSentinelPool = new JedisSentinelPool(redisMaster, sentinels);
            jedis = jedisSentinelPool.getResource();
            jedisPipeline = jedis.pipelined();

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

                jedisPipeline.incrByFloat(key, doubleValue);
                jedisPipeline.expireAt(key, expireAtValue);

                logger.debug("Record written to Redis: key={}, value={}", key, value);
            }

        } catch (Exception e) {
            final String message = "Failed to write record to Redis: key=" + key + ", value=" + value;
            logger.error(message, e);
            throw new RetriableException(message, e);
        }

        jedisPipeline.sync();
    }

    @Override
    public void stop() {
        logger.info("Stopping Redis sink task");
        if (jedisPipeline != null) {
            jedisPipeline.close();
        }
        if (jedis != null) {
            jedis.close();
        }
        if (jedisSentinelPool != null) {
            jedisSentinelPool.destroy();
        }
    }

    @Override
    public String version() {
        return RedisSinkConnector.VERSION;
    }
}
