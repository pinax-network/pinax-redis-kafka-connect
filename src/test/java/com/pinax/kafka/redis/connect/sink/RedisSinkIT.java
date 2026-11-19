package com.pinax.kafka.redis.connect.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisAccessControlException;
import redis.clients.jedis.exceptions.JedisDataException;

/**
 * Integration tests against a real Redis (Testcontainers), validating the
 * transaction semantics {@link RedisSinkTask} relies on.
 *
 * <p>These exercise Jedis directly rather than driving the task end-to-end:
 * the task uses {@link redis.clients.jedis.JedisSentinelPool}, and a Sentinel
 * topology under Testcontainers returns the master's container-internal address,
 * which the host JVM cannot route without fragile fixed-port/announce-ip tricks.
 * Testing the transaction primitives directly keeps the integration test reliable
 * while still covering the behaviour the task's exception handling depends on.
 *
 * <p>Self-skips (rather than fails) when no Docker daemon is available, except in
 * CI (environment variable {@code CI} set), where that fails the build.
 */
class RedisSinkIT {

    private static GenericContainer<?> redis;
    private static Jedis jedis;

    @BeforeAll
    static void startRedis() {
        boolean dockerAvailable = DockerClientFactory.instance().isDockerAvailable();
        // CI must run these: a skip there once went unnoticed for a long time.
        String ci = System.getenv("CI");
        if (ci != null && !ci.isEmpty()) {
            assertTrue(dockerAvailable, "Docker is not available in CI, the integration tests would be skipped");
        }
        assumeTrue(dockerAvailable, "Docker is not available");
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
    void clientConfigAuthenticatesAgainstARedisThatRequiresAPassword() {
        try (GenericContainer<?> protectedRedis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
                .withCommand("redis-server", "--requirepass", "s3cret")
                .withExposedPorts(6379)
                .waitingFor(Wait.forListeningPort())) {
            protectedRedis.start();
            HostAndPort address = new HostAndPort(protectedRedis.getHost(), protectedRedis.getMappedPort(6379));

            // A password without a user authenticates as the default user.
            try (Jedis authenticated = new Jedis(address, RedisSinkTask.clientConfig("", "s3cret"))) {
                assertEquals("PONG", authenticated.ping());
            }
            try (Jedis authenticated = new Jedis(address, RedisSinkTask.clientConfig("default", "s3cret"))) {
                assertEquals("PONG", authenticated.ping());
            }
            // No password: no authentication is attempted, and Redis refuses commands.
            try (Jedis anonymous = new Jedis(address, RedisSinkTask.clientConfig("", ""))) {
                assertThrows(JedisDataException.class, anonymous::ping);
            }
            // A wrong password is the permanent error the task fails fast on.
            assertThrows(JedisAccessControlException.class, () -> {
                try (Jedis wrong = new Jedis(address, RedisSinkTask.clientConfig("", "wrong"))) {
                    wrong.ping();
                }
            });
        }
    }

    @Test
    void sentinelCredentialsCheckFailsOnRefusedCredentialsAndSkipsUnreachableSentinels() {
        // A Redis with requirepass stands in for a Sentinel: the check only
        // authenticates and pings, which a Sentinel answers the same way.
        try (GenericContainer<?> protectedSentinel = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
                .withCommand("redis-server", "--requirepass", "s3cret")
                .withExposedPorts(6379)
                .waitingFor(Wait.forListeningPort())) {
            protectedSentinel.start();
            HostAndPort reachable = new HostAndPort(protectedSentinel.getHost(), protectedSentinel.getMappedPort(6379));
            // Nothing listens there; connecting is refused at once.
            HostAndPort unreachable = new HostAndPort("127.0.0.1", 1);
            Set<HostAndPort> sentinels = new LinkedHashSet<HostAndPort>(List.of(unreachable, reachable));

            RedisSinkTask.checkSentinelCredentials(sentinels, RedisSinkTask.clientConfig("", "s3cret"));
            assertThrows(JedisAccessControlException.class,
                    () -> RedisSinkTask.checkSentinelCredentials(sentinels, RedisSinkTask.clientConfig("", "wrong")));
            assertThrows(JedisAccessControlException.class,
                    () -> RedisSinkTask.checkSentinelCredentials(sentinels, RedisSinkTask.clientConfig("", "")));
            // No Sentinel answers: left to the pool, which reports the outage.
            RedisSinkTask.checkSentinelCredentials(Set.of(unreachable), RedisSinkTask.clientConfig("", "wrong"));
        }
    }

    @Test
    void transactionAccumulatesSetsTtlAndStoresOffsets() {
        String key = "team:accumulate";
        String offsetsKey = "offsets:accumulate";
        jedis.del(key, offsetsKey);
        long expireAt = Instant.now().getEpochSecond() + 3600;

        jedis.watch(offsetsKey);
        assertTrue(jedis.hgetAll(offsetsKey).isEmpty());
        Transaction transaction = jedis.multi();
        Response<Double> first = transaction.incrByFloat(key, 100.0);
        transaction.expireAt(key, expireAt);
        Response<Double> second = transaction.incrByFloat(key, 50.0);
        transaction.hset(offsetsKey, "0", "41");
        assertNotNull(transaction.exec());

        assertEquals(100.0, first.get(), 0.0001);
        assertEquals(150.0, second.get(), 0.0001);
        assertTrue(jedis.ttl(key) > 0, "expireAt should leave a positive TTL");
        assertEquals("41", jedis.hget(offsetsKey, "0"));
    }

    @Test
    void execAppliesNothingWhenWatchedOffsetsChanged() {
        String key = "team:conflict";
        String offsetsKey = "offsets:conflict";
        jedis.del(key, offsetsKey);

        jedis.watch(offsetsKey);
        jedis.hgetAll(offsetsKey);

        // Another task stores its offsets between this task's WATCH and EXEC.
        try (Jedis other = new Jedis(redis.getHost(), redis.getMappedPort(6379))) {
            other.hset(offsetsKey, "0", "7");
        }

        Transaction transaction = jedis.multi();
        transaction.incrByFloat(key, 100.0);
        transaction.hset(offsetsKey, "0", "5");

        // This is what RedisSinkTask turns into a RetriableException.
        assertNull(transaction.exec());
        assertFalse(jedis.exists(key), "the increment must not be applied");
        assertEquals("7", jedis.hget(offsetsKey, "0"));
    }

    @Test
    void getThrowsWhenKeyHasWrongTypeAndTheRestIsStillApplied() {
        String key = "team:wrongtype";
        String offsetsKey = "offsets:wrongtype";
        jedis.del(key, offsetsKey);
        jedis.rpush(key, "not-a-number"); // key is now a list, not a number

        Transaction transaction = jedis.multi();
        Response<Double> bad = transaction.incrByFloat(key, 1.0);
        transaction.hset(offsetsKey, "0", "3");
        List<Object> results = transaction.exec();

        // EXEC succeeds and returns the rejected command as an exception among its
        // results, which RedisSinkTask checks for every command of the batch.
        assertTrue(results.get(0) instanceof JedisDataException);
        assertFalse(results.get(1) instanceof JedisDataException);

        // The same error surfaces at get().
        assertThrows(JedisDataException.class, bad::get);
        // Redis does not roll back: the offsets of the batch are stored regardless.
        assertEquals("3", jedis.hget(offsetsKey, "0"));
    }

    @Test
    void getThrowsWhenValueIsNotAValidFloat() {
        String key = "team:notfloat";
        jedis.set(key, "abc");

        Transaction transaction = jedis.multi();
        Response<Double> bad = transaction.incrByFloat(key, 1.0);
        transaction.exec();

        assertThrows(JedisDataException.class, bad::get);
    }
}
