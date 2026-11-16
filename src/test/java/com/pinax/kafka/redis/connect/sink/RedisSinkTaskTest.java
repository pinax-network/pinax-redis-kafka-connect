package com.pinax.kafka.redis.connect.sink;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisAccessControlException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

/**
 * Unit tests for {@link RedisSinkTask}. The task constructs its
 * {@link JedisSentinelPool} and {@link MailSender} directly, so we intercept
 * those constructions with Mockito (no real Redis or HTTP) and drive behaviour
 * through the mocked {@link Transaction}. End-to-end Redis semantics are covered
 * separately by {@code RedisSinkIT}.
 */
class RedisSinkTaskTest {

    private static final String OFFSETS_KEY = "kafka-connect:redis-sink:offsets:usage";
    private static final TopicPartition PARTITION = new TopicPartition("usage", 0);
    private static final String CREDITS_JSON = "{\"billed_credits\":1.0,\"expiration\":9999999999}";

    private Jedis jedis;
    private Transaction transaction;
    private SinkTaskContext context;
    private RedisSinkTask task;

    @BeforeEach
    void setUp() {
        jedis = mock(Jedis.class);
        transaction = mock(Transaction.class);
        when(jedis.multi()).thenReturn(transaction);
        when(jedis.hgetAll(anyString())).thenReturn(new HashMap<>());
        when(transaction.exec()).thenReturn(List.of());
        context = mock(SinkTaskContext.class);
        task = new RedisSinkTask();
        task.initialize(context);
    }

    private static Map<String, String> props() {
        Map<String, String> props = new HashMap<>();
        props.put(RedisSinkConfig.MASTER, "mymaster");
        props.put(RedisSinkConfig.HOSTS, "localhost:26379");
        props.put(RedisSinkConfig.FROM, "info@pinax.network");
        props.put(RedisSinkConfig.MAILCHIMP_API_KEY, "key");
        props.put(RedisSinkConfig.TEMPLATE_SLUG, "slug");
        return props;
    }

    private static SinkRecord record(String key, String json) {
        return record(key, json, 0L);
    }

    private static SinkRecord record(String key, String json, long offset) {
        return new SinkRecord("usage", 0, null, key, null, json, offset);
    }

    /** The offset of the last record applied to partition 0, as stored in Redis. */
    private void storedOffset(long offset) {
        Map<String, String> offsets = new HashMap<>();
        offsets.put("0", Long.toString(offset));
        when(jedis.hgetAll(OFFSETS_KEY)).thenReturn(offsets);
    }

    @SuppressWarnings("unchecked")
    private Response<Double> stubIncrByFloat(double newValue) {
        Response<Double> response = mock(Response.class);
        when(response.get()).thenReturn(newValue);
        when(transaction.incrByFloat(anyString(), anyDouble())).thenReturn(response);
        when(transaction.expireAt(anyString(), anyLong())).thenReturn(mock(Response.class));
        return response;
    }

    /** Opens the construction mocks for a connected (healthy) Redis + MailSender. */
    private interface Body {
        void run(MailSender mailSender) throws Exception;
    }

    private void withConnectedRedis(Body body) {
        try (MockedConstruction<MailSender> mailMock = mockConstruction(MailSender.class,
                (m, ctx) -> when(m.CreateUsageMailRequest(anyString(), anyString(), any(MailContent.class)))
                        .thenReturn(new HttpPost("https://mandrill.test")));
             MockedConstruction<JedisSentinelPool> poolMock = mockConstruction(JedisSentinelPool.class,
                (m, ctx) -> when(m.getResource()).thenReturn(jedis))) {

            task.start(props());
            body.run(mailMock.constructed().get(0));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void put_emptyRecords_isNoOp() {
        assertDoesNotThrow(() -> task.put(Collections.emptyList()));
    }

    @Test
    void put_malformedJson_throwsDataException() {
        withConnectedRedis(mailSender -> {
            List<SinkRecord> records = List.of(record("team:1", "this is not json"));
            assertThrows(DataException.class, () -> task.put(records));
        });
    }

    @Test
    void put_execConnectionError_throwsRetriable() {
        withConnectedRedis(mailSender -> {
            stubIncrByFloat(1.0);
            when(transaction.exec()).thenThrow(new JedisConnectionException("connection reset"));

            List<SinkRecord> records = List.of(
                    record("team:1", "{\"billed_credits\":1.0,\"expiration\":9999999999}"));
            assertThrows(RetriableException.class, () -> task.put(records));
        });
    }

    @Test
    void put_execAccessControlError_throwsConnectException() {
        withConnectedRedis(mailSender -> {
            stubIncrByFloat(1.0);
            when(transaction.exec()).thenThrow(new JedisAccessControlException("NOPERM"));

            List<SinkRecord> records = List.of(
                    record("team:1", "{\"billed_credits\":1.0,\"expiration\":9999999999}"));
            assertThrows(ConnectException.class, () -> task.put(records));
        });
    }

    @Test
    void put_execDataError_throwsDataException() {
        withConnectedRedis(mailSender -> {
            stubIncrByFloat(1.0);
            when(transaction.exec()).thenThrow(new JedisDataException("ERR value is not a valid float"));

            List<SinkRecord> records = List.of(
                    record("team:1", "{\"billed_credits\":1.0,\"expiration\":9999999999}"));
            assertThrows(DataException.class, () -> task.put(records));
        });
    }

    @Test
    void put_perCommandErrorSurfacedAtGet_throwsDataException() {
        // Jedis raises per-command errors (WRONGTYPE, bad float) at Response.get(),
        // not at exec(). prepareMailRequests reads get() outside its catch so the
        // error reaches put()'s JedisDataException handler -> DataException.
        withConnectedRedis(mailSender -> {
            @SuppressWarnings("unchecked")
            Response<Double> response = mock(Response.class);
            when(response.get()).thenThrow(new JedisDataException("WRONGTYPE"));
            when(transaction.incrByFloat(anyString(), anyDouble())).thenReturn(response);
            when(transaction.expireAt(anyString(), anyLong())).thenReturn(mock(Response.class));

            List<SinkRecord> records = List.of(
                    record("team:1", "{\"billed_credits\":1.0,\"expiration\":9999999999}"));
            assertThrows(DataException.class, () -> task.put(records));
        });
    }

    @Test
    void put_thresholdCrossed_sendsUsageMail() {
        withConnectedRedis(mailSender -> {
            stubIncrByFloat(6000.0); // crosses the 50% milestone of 10000 (=5000)

            String json = "{\"billed_credits\":6000.0,\"expiration\":9999999999,"
                    + "\"included_credits\":10000,\"credit_cutoff\":0,"
                    + "\"team_billing_email\":\"team@acme.test\",\"team_name\":\"Acme\","
                    + "\"team_plan\":\"Pro\"}";
            task.put(List.of(record("team:1", json)));

            verify(mailSender).CreateUsageMailRequest(eq("team@acme.test"), anyString(), any(MailContent.class));
            verify(mailSender).SendUsageMailRequest(any(HttpPost.class));
        });
    }

    @Test
    void put_includedCreditsEqualsCreditCutoff_stillSendsMail() {
        // Regression guard: credit_cutoff no longer gates notifications, so a team
        // whose included_credits equals credit_cutoff must still be emailed.
        withConnectedRedis(mailSender -> {
            stubIncrByFloat(6000.0); // crosses the 50% milestone of 10000

            String json = "{\"billed_credits\":6000.0,\"expiration\":9999999999,"
                    + "\"included_credits\":10000,\"credit_cutoff\":10000,"
                    + "\"team_billing_email\":\"team@acme.test\",\"team_name\":\"Acme\","
                    + "\"team_plan\":\"Pro\"}";
            task.put(List.of(record("team:1", json)));

            verify(mailSender).CreateUsageMailRequest(eq("team@acme.test"), anyString(), any(MailContent.class));
            verify(mailSender).SendUsageMailRequest(any(HttpPost.class));
        });
    }

    @Test
    void put_batchCrossesMultipleMilestones_sendsExactlyOneMail() {
        // 0 -> 16000 against an allowance of 10000 crosses 50/75/90/100/150% in one
        // batch; only a single email (the highest milestone) must be sent.
        withConnectedRedis(mailSender -> {
            stubIncrByFloat(16000.0);

            String json = "{\"billed_credits\":16000.0,\"expiration\":9999999999,"
                    + "\"included_credits\":10000,\"credit_cutoff\":0,"
                    + "\"team_billing_email\":\"team@acme.test\",\"team_name\":\"Acme\","
                    + "\"team_plan\":\"Pro\"}";
            task.put(List.of(record("team:1", json)));

            verify(mailSender, times(1)).CreateUsageMailRequest(anyString(), anyString(), any(MailContent.class));
            verify(mailSender, times(1)).SendUsageMailRequest(any(HttpPost.class));
        });
    }

    @Test
    void put_malformedNotificationField_skipsMailButSucceeds() {
        // billed_credits/expiration parse fine and the Redis write succeeds, but a
        // missing notification field must only skip the email, not fail the batch.
        withConnectedRedis(mailSender -> {
            stubIncrByFloat(6000.0);

            String json = "{\"billed_credits\":6000.0,\"expiration\":9999999999}"; // no included_credits etc.
            assertDoesNotThrow(() -> task.put(List.of(record("team:1", json))));

            verify(mailSender, never()).CreateUsageMailRequest(anyString(), anyString(), any(MailContent.class));
        });
    }

    @Test
    void put_storesOffsetOfLastRecordPerPartitionInTheTransaction() {
        withConnectedRedis(mailSender -> {
            stubIncrByFloat(1.0);

            task.put(List.of(record("team:1", CREDITS_JSON, 7L), record("team:2", CREDITS_JSON, 9L)));

            verify(jedis).watch(OFFSETS_KEY);
            verify(transaction, times(2)).incrByFloat(anyString(), anyDouble());
            verify(transaction).hset(OFFSETS_KEY, "0", "9");
            verify(transaction).exec();
        });
    }

    @Test
    void put_skipsRecordsAtOrBelowTheStoredOffset() {
        withConnectedRedis(mailSender -> {
            stubIncrByFloat(1.0);
            storedOffset(5L);

            task.put(List.of(record("team:1", CREDITS_JSON, 4L), record("team:2", CREDITS_JSON, 5L),
                    record("team:3", CREDITS_JSON, 6L)));

            verify(transaction, times(1)).incrByFloat(anyString(), anyDouble());
            verify(transaction).incrByFloat("team:3", 1.0);
            verify(transaction).hset(OFFSETS_KEY, "0", "6");
            verify(context, never()).offset(any(TopicPartition.class), anyLong());
        });
    }

    @Test
    void put_allRecordsAlreadyApplied_writesNothingAndSendsNoMail() {
        withConnectedRedis(mailSender -> {
            storedOffset(5L);

            String json = "{\"billed_credits\":6000.0,\"expiration\":9999999999,"
                    + "\"included_credits\":10000,\"credit_cutoff\":0,"
                    + "\"team_billing_email\":\"team@acme.test\",\"team_name\":\"Acme\","
                    + "\"team_plan\":\"Pro\"}";
            task.put(List.of(record("team:1", json, 5L)));

            verify(jedis, never()).multi();
            verify(jedis).unwatch();
            verify(mailSender, never()).CreateUsageMailRequest(anyString(), anyString(), any(MailContent.class));
        });
    }

    @Test
    void put_firstBatchBeyondStoredOffset_rewindsAndLeavesThePartitionOut() {
        // Redis was restored from a snapshot: its counters and offsets are older than
        // what the consumer group has committed.
        withConnectedRedis(mailSender -> {
            stubIncrByFloat(1.0);
            storedOffset(5L);

            task.put(List.of(record("team:1", CREDITS_JSON, 9L)));

            verify(context).offset(PARTITION, 6L);
            verify(jedis, never()).multi();

            // Connect delivers the partition again from the rewound position.
            task.put(List.of(record("team:1", CREDITS_JSON, 7L), record("team:1", CREDITS_JSON, 9L)));

            verify(context, times(1)).offset(any(TopicPartition.class), anyLong());
            verify(transaction, times(2)).incrByFloat("team:1", 1.0);
            verify(transaction).hset(OFFSETS_KEY, "0", "9");
        });
    }

    @Test
    void put_firstBatchFollowsStoredOffset_doesNotRewind() {
        withConnectedRedis(mailSender -> {
            stubIncrByFloat(1.0);
            storedOffset(5L);

            task.put(List.of(record("team:1", CREDITS_JSON, 6L)));

            verify(context, never()).offset(any(TopicPartition.class), anyLong());
            verify(transaction).hset(OFFSETS_KEY, "0", "6");
        });
    }

    @Test
    void put_noStoredOffset_doesNotRewind() {
        // First start with this version, or an empty Redis: the consumer group decides.
        withConnectedRedis(mailSender -> {
            stubIncrByFloat(1.0);

            task.put(List.of(record("team:1", CREDITS_JSON, 42L)));

            verify(context, never()).offset(any(TopicPartition.class), anyLong());
            verify(transaction).hset(OFFSETS_KEY, "0", "42");
        });
    }

    @Test
    void open_checksTheStoredOffsetAgainForReassignedPartitions() {
        withConnectedRedis(mailSender -> {
            stubIncrByFloat(1.0);
            storedOffset(5L);

            task.put(List.of(record("team:1", CREDITS_JSON, 6L)));
            // A gap that is not checked: only the first batch after an assignment is.
            task.put(List.of(record("team:1", CREDITS_JSON, 9L)));
            verify(context, never()).offset(any(TopicPartition.class), anyLong());

            task.open(List.of(PARTITION));
            task.put(List.of(record("team:1", CREDITS_JSON, 12L)));

            verify(context).offset(PARTITION, 6L);
        });
    }

    @Test
    void put_offsetsChangedConcurrently_throwsRetriable() {
        // EXEC returns null when a WATCHed key was modified: nothing was applied.
        withConnectedRedis(mailSender -> {
            stubIncrByFloat(1.0);
            when(transaction.exec()).thenReturn(null);

            assertThrows(RetriableException.class, () -> task.put(List.of(record("team:1", CREDITS_JSON))));
        });
    }

    @Test
    void put_invalidStoredOffset_throwsDataException() {
        withConnectedRedis(mailSender -> {
            Map<String, String> offsets = new HashMap<>();
            offsets.put("0", "not-a-number");
            when(jedis.hgetAll(OFFSETS_KEY)).thenReturn(offsets);

            assertThrows(DataException.class, () -> task.put(List.of(record("team:1", CREDITS_JSON))));
        });
    }

    @Test
    void start_accessControlError_throwsConnectException() {
        try (MockedConstruction<MailSender> mailMock = mockConstruction(MailSender.class);
             MockedConstruction<JedisSentinelPool> poolMock = mockConstruction(JedisSentinelPool.class,
                (m, ctx) -> when(m.getResource()).thenThrow(new JedisAccessControlException("WRONGPASS")))) {

            assertThrows(ConnectException.class, () -> task.start(props()));
        }
    }

    @Test
    void start_connectionError_isToleratedAndRetriedLater() {
        try (MockedConstruction<MailSender> mailMock = mockConstruction(MailSender.class);
             MockedConstruction<JedisSentinelPool> poolMock = mockConstruction(JedisSentinelPool.class,
                (m, ctx) -> when(m.getResource()).thenThrow(new JedisConnectionException("all sentinels down")))) {

            // A transient outage at startup must not fail the task.
            assertDoesNotThrow(() -> task.start(props()));
        }
    }
}
