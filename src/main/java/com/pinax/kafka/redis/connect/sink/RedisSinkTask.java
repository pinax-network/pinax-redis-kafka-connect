package com.pinax.kafka.redis.connect.sink;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

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
import redis.clients.jedis.exceptions.JedisAccessControlException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;

public class RedisSinkTask extends SinkTask {
    private final Logger logger = LoggerFactory.getLogger(RedisSinkConnector.class);

    private Set<String> sentinels = new HashSet<String>();

    private JedisSentinelPool jedisSentinelPool = null;
    private Jedis jedis = null;
    private Pipeline jedisPipeline = null;

    private List<String> redisHosts;
    private String redisMaster;

    private void createRedisConnection() {
        try {
            jedisSentinelPool = new JedisSentinelPool(redisMaster, sentinels);
            jedis = jedisSentinelPool.getResource();
            jedisPipeline = jedis.pipelined();

            logger.info("Redis connection created");
        } catch (Exception e) {
            logger.error("Failed to create Redis connection", e);
        }
    }

    private void closeRedisConnection() {
        if (jedisPipeline != null) {
            jedisPipeline.close();
        }
        if (jedis != null) {
            jedis.close();
        }
        if (jedisSentinelPool != null) {
            jedisSentinelPool.close();
        }

        logger.info("Redis connection closed");
    }

    private void reconnectToRedis() {
        closeRedisConnection();
        createRedisConnection();
    }

    @Override
    public void start(Map<String, String> properties) {
        logger.info("Starting Redis sink task {}", properties);

        AbstractConfig config = new AbstractConfig(RedisSinkConfig.CONFIG_DEF, properties);

        // Prepare Redis connection
        redisHosts = config.getList(RedisSinkConfig.HOSTS);
        redisMaster = config.getString(RedisSinkConfig.MASTER);

        for (String redisHostPort : redisHosts) {
            sentinels.add(redisHostPort);
        }

        createRedisConnection();
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.size() > 0) {
            logger.debug("Received records from Connect");
        }

        String key = null;
        String value = null;

        for (SinkRecord record : records) {
            try {
                logger.debug("Processing record: {}", record);
                key = record.key() == null ? "" : record.key().toString();
                value = record.value() == null ? "" : record.value().toString();

                // value:expireAt (should be good since it's validated in the config)
                String[] parts = value.split(":");
                if (parts.length != 2) {
                    throw new DataException("Invalid value format: " + value);
                }

                double doubleValue = Double.parseDouble(parts[0]);
                long expireAtValue = Long.parseLong(parts[1]);

                jedisPipeline.incrByFloat(key, doubleValue);
                jedisPipeline.expireAt(key, expireAtValue);

                logger.debug("Record written to Redis: key={}, value={}", key, value);
            } catch (JedisConnectionException e) {
                logger.error("Redis connection error", e);
                reconnectToRedis();
                throw new RetriableException("Redis connection error", e);
            } catch (JedisAccessControlException e) {
                logger.error("Redis access control error", e);
                reconnectToRedis();
                throw new RetriableException("Redis access control error", e);
            } catch (Exception e) {
                logger.error("Data or parsing error", e);
                throw new DataException("Data or parsing error", e);
            }
        }

        try {
            jedisPipeline.sync();
        } catch (JedisConnectionException e) {
            logger.error("Redis connection error", e);
            reconnectToRedis();
            throw new RetriableException("Redis connection error", e);
        } catch (JedisAccessControlException e) {
            logger.error("Redis access control error", e);
            reconnectToRedis();
            throw new RetriableException("Redis access control error", e);
        } catch (Exception e) {
            logger.error("Data or parsing error", e);
            throw new DataException("Data or parsing error", e);
        }
    }

    @Override
    public void stop() {
        logger.info("Stopping Redis sink task");
        closeRedisConnection();
    }

    @Override
    public String version() {
        return RedisSinkConnector.VERSION;
    }
}
