package com.pinax.kafka.redis.connect.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Instant;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisDataException;

/**
 * Integration tests against a real Redis (Testcontainers), validating the
 * pipeline semantics {@link RedisSinkTask} relies on.
 *
 * <p>These exercise Jedis directly rather than driving the task end-to-end:
 * the task uses {@link redis.clients.jedis.JedisSentinelPool}, and a Sentinel
 * topology under Testcontainers returns the master's container-internal address,
 * which the host JVM cannot route without fragile fixed-port/announce-ip tricks.
 * Testing the pipeline primitives directly keeps the integration test reliable
 * while still covering the behaviour the task's exception handling depends on.
 *
 * <p>Self-skips (rather than fails) when no Docker daemon is available.
 */
class RedisSinkIT {

    private static GenericContainer<?> redis;
    private static Jedis jedis;

    @BeforeAll
    static void startRedis() {
        assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is not available");
        redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
                .withExposedPorts(6379)
                .waitingFor(Wait.forListeningPort());
        redis.start();
        jedis = new Jedis(redis.getHost(), redis.getMappedPort(6379));
    }

    @AfterAll
    static void stopRedis() {
        if (jedis != null) {
            jedis.close();
        }
        if (redis != null) {
            redis.stop();
        }
    }

    @Test
    void pipelinedIncrByFloatAccumulatesAndSetsTtl() {
        String key = "team:accumulate";
        jedis.del(key);
        long expireAt = Instant.now().getEpochSecond() + 3600;

        Pipeline pipeline = jedis.pipelined();
        Response<Double> first = pipeline.incrByFloat(key, 100.0);
        pipeline.expireAt(key, expireAt);
        Response<Double> second = pipeline.incrByFloat(key, 50.0);
        pipeline.sync();

        assertEquals(100.0, first.get(), 0.0001);
        assertEquals(150.0, second.get(), 0.0001);
        assertTrue(jedis.ttl(key) > 0, "expireAt should leave a positive TTL");
    }

    @Test
    void getThrowsWhenKeyHasWrongType() {
        String key = "team:wrongtype";
        jedis.del(key);
        jedis.rpush(key, "not-a-number"); // key is now a list, not a number

        Pipeline pipeline = jedis.pipelined();
        Response<Double> bad = pipeline.incrByFloat(key, 1.0);
        pipeline.sync();

        // Crucially: the error surfaces at get(), NOT at sync(). This is the exact
        // assumption RedisSinkTask.prepareMailRequests relies on by reading get()
        // outside its JSONException catch.
        assertThrows(JedisDataException.class, bad::get);
    }

    @Test
    void getThrowsWhenValueIsNotAValidFloat() {
        String key = "team:notfloat";
        jedis.set(key, "abc");

        Pipeline pipeline = jedis.pipelined();
        Response<Double> bad = pipeline.incrByFloat(key, 1.0);
        pipeline.sync();

        assertThrows(JedisDataException.class, bad::get);
    }
}
