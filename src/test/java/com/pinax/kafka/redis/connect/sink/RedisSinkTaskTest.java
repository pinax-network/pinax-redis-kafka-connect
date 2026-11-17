package com.pinax.kafka.redis.connect.sink;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
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
import java.util.Set;

import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.common.metrics.Sensor;
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
    private Sensor rejectedCommands;
    private RedisSinkTask task;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        jedis = mock(Jedis.class);
        transaction = mock(Transaction.class);
        when(jedis.multi()).thenReturn(transaction);
        when(jedis.hgetAll(anyString())).thenReturn(new HashMap<>());
        when(transaction.exec()).thenReturn(List.of());
        when(transaction.expireAt(anyString(), anyLong())).thenReturn(mock(Response.class));
        when(transaction.hset(anyString(), anyString(), anyString())).thenReturn(mock(Response.class));
        context = mock(SinkTaskContext.class);
        PluginMetrics metrics = mock(PluginMetrics.class);
        rejectedCommands = mock(Sensor.class);
        when(context.pluginMetrics()).thenReturn(metrics);
        when(metrics.addSensor(startsWith("rejected-commands-"))).thenReturn(rejectedCommands);
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
    void put_malformedJson_throwsDataExceptionAndWritesNothing() {
        withConnectedRedis(mailSender -> {
            List<SinkRecord> records = List.of(
                    record("team:1", CREDITS_JSON, 7L), record("team:2", "this is not json", 8L));

            assertThrows(DataException.class, () -> task.put(records));

            verify(jedis, never()).multi();
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
            verify(context).timeout(5000L);
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
    void put_redisNotAcceptingWrites_failsTheTask() {
        // Left to whatever restarts failed tasks, with its own backoff.
        withConnectedRedis(mailSender -> {
            when(jedis.hgetAll(anyString()))
                    .thenThrow(new JedisDataException("LOADING Redis is loading the dataset in memory"));

            assertThrows(DataException.class, () -> task.put(List.of(record("team:1", CREDITS_JSON))));
            verify(context, never()).timeout(anyLong());
        });
    }

    @Test
    void put_malformedRecordAtOrBelowTheStoredOffset_isNotParsed() {
        // The way past a record that cannot be parsed: the stored offset was moved
        // to it by hand. Kafka still delivers it, the consumer group is before it.
        withConnectedRedis(mailSender -> {
            stubIncrByFloat(1.0);
            storedOffset(8L);

            assertDoesNotThrow(() -> task.put(List.of(
                    record("team:1", "this is not json", 8L), record("team:2", CREDITS_JSON, 9L))));

            verify(transaction, times(1)).incrByFloat(eq("team:2"), anyDouble());
            verify(transaction).hset(OFFSETS_KEY, "0", "9");
        });
    }

    @Test
    void start_metricCannotBeRegistered_isTolerated() {
        when(context.pluginMetrics()).thenThrow(new IllegalArgumentException("A metric named x already exists"));
        withConnectedRedis(mailSender -> {
            stubIncrByFloat(1.0);

            assertDoesNotThrow(() -> task.put(List.of(record("team:1", CREDITS_JSON))));
        });
    }

    @Test
    void assignedPartition_kafkaDoesNotHaveTheStoredOffset_failsTheTask() {
        // The hash is from another topic or cluster: offset 5000 is beyond the end of
        // this partition, so the consumer starts over at the earliest record.
        withConnectedRedis(mailSender -> {
            storedOffset(5000L);

            task.open(List.of(PARTITION));
            task.put(Collections.emptyList());
            verify(context).offset(PARTITION, 5001L);

            assertThrows(ConnectException.class, () -> task.put(List.of(record("team:1", CREDITS_JSON, 0L))));
            verify(jedis, never()).multi();
        });
    }

    @Test
    void put_rejectedIncrement_isCountedAndTheRestOfTheBatchIsNotified() {
        // Redis does not roll the transaction back: the rejected command's Response
        // throws at get(), the other record is applied and gets its notification.
        withConnectedRedis(mailSender -> {
            @SuppressWarnings("unchecked")
            Response<Double> rejected = mock(Response.class);
            when(rejected.get()).thenThrow(new JedisDataException("WRONGTYPE"));
            @SuppressWarnings("unchecked")
            Response<Double> applied = mock(Response.class);
            when(applied.get()).thenReturn(55.0);
            when(transaction.incrByFloat(eq("team:1"), anyDouble())).thenReturn(rejected);
            when(transaction.incrByFloat(eq("team:2"), anyDouble())).thenReturn(applied);

            String json = "{\"billed_credits\":10.0,\"expiration\":9999999999,\"included_credits\":100,"
                    + "\"team_billing_email\":\"a@b.c\",\"team_name\":\"Acme\",\"team_plan\":\"Pro\"}";
            assertDoesNotThrow(() -> task.put(List.of(record("team:1", json, 1L), record("team:2", json, 2L))));

            verify(rejectedCommands).record();
            verify(mailSender, times(1)).CreateUsageMailRequest(anyString(), anyString(), any(MailContent.class));
            verify(mailSender, times(1)).SendUsageMailRequest(any(HttpPost.class));
        });
    }

    @Test
    void put_rejectedExpiration_isCountedAndTheTaskCarriesOn() {
        withConnectedRedis(mailSender -> {
            stubIncrByFloat(1.0);
            @SuppressWarnings("unchecked")
            Response<Long> rejected = mock(Response.class);
            when(rejected.get()).thenThrow(new JedisDataException("ERR invalid expire time in 'expireat' command"));
            when(transaction.expireAt(anyString(), anyLong())).thenReturn(rejected);

            assertDoesNotThrow(() -> task.put(List.of(record("team:1", CREDITS_JSON))));

            verify(rejectedCommands).record();
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
    void assignedPartition_isMovedToTheStoredOffsetAndLeftOutOfTheBatch() {
        // Redis was restored from a snapshot: its counters and offsets are older than
        // what the consumer group has committed.
        withConnectedRedis(mailSender -> {
            stubIncrByFloat(1.0);
            storedOffset(5L);

            task.open(List.of(PARTITION));
            task.put(List.of(record("team:1", CREDITS_JSON, 9L)));

            verify(context).offset(PARTITION, 6L);
            verify(jedis, never()).multi();

            // Connect delivers the partition again from the new position.
            task.put(List.of(record("team:1", CREDITS_JSON, 7L), record("team:1", CREDITS_JSON, 9L)));

            verify(context, times(1)).offset(any(TopicPartition.class), anyLong());
            verify(transaction, times(2)).incrByFloat("team:1", 1.0);
            verify(transaction).hset(OFFSETS_KEY, "0", "9");
        });
    }

    @Test
    void assignedPartition_withoutRecords_isMovedToTheStoredOffset() {
        // The consumer group is at the end of the partition and nothing is produced
        // to it: no record will ever arrive to trigger the replay.
        withConnectedRedis(mailSender -> {
            storedOffset(5L);

            task.open(List.of(PARTITION));
            task.put(Collections.emptyList());

            verify(context).offset(PARTITION, 6L);
            verify(jedis, never()).multi();

            task.put(Collections.emptyList());
            verify(context, times(1)).offset(any(TopicPartition.class), anyLong());
        });
    }

    @Test
    void assignedPartition_withoutStoredOffset_staysWhereTheConsumerGroupHasIt() {
        // First start with this version, or an empty Redis.
        withConnectedRedis(mailSender -> {
            stubIncrByFloat(1.0);

            task.open(List.of(PARTITION));
            task.put(List.of(record("team:1", CREDITS_JSON, 42L)));

            verify(context, never()).offset(any(TopicPartition.class), anyLong());
            verify(transaction).hset(OFFSETS_KEY, "0", "42");
        });
    }

    @Test
    void assignedPartition_batchContinuesAfterTheStoredOffset_isNotMoved() {
        // The usual case: nothing was lost, so nothing has to be read again.
        withConnectedRedis(mailSender -> {
            stubIncrByFloat(1.0);
            storedOffset(5L);

            task.open(List.of(PARTITION));
            task.put(List.of(record("team:1", CREDITS_JSON, 6L), record("team:1", CREDITS_JSON, 7L)));

            verify(context, never()).offset(any(TopicPartition.class), anyLong());
            verify(transaction, times(2)).incrByFloat("team:1", 1.0);
            verify(transaction).hset(OFFSETS_KEY, "0", "7");
        });
    }

    @Test
    void assignedPartition_batchOverlapsTheStoredOffset_isNotMovedAndSkipsWhatIsApplied() {
        withConnectedRedis(mailSender -> {
            stubIncrByFloat(1.0);
            storedOffset(5L);

            task.open(List.of(PARTITION));
            task.put(List.of(record("team:1", CREDITS_JSON, 5L), record("team:1", CREDITS_JSON, 6L)));

            verify(context, never()).offset(any(TopicPartition.class), anyLong());
            verify(transaction, times(1)).incrByFloat("team:1", 1.0);
            verify(transaction).hset(OFFSETS_KEY, "0", "6");
        });
    }

    @Test
    void assignedPartition_batchEntirelyApplied_isMovedForwardToTheStoredOffset() {
        // The consumer group is behind Redis: jump ahead instead of reading through.
        withConnectedRedis(mailSender -> {
            storedOffset(5L);

            task.open(List.of(PARTITION));
            task.put(List.of(record("team:1", CREDITS_JSON, 2L), record("team:1", CREDITS_JSON, 3L)));

            verify(context).offset(PARTITION, 6L);
            verify(jedis, never()).multi();
        });
    }

    @Test
    void reconnect_afterASeekWasRequested_movesThePartitionAgain() {
        // The first attempt asks for a seek to 26 and then fails. Connect carries the
        // seek out anyway. The retry finds a Redis that holds 15: the batch in hand
        // would continue there, but the consumer is at 26 by now.
        withConnectedRedis(mailSender -> {
            stubIncrByFloat(1.0);
            when(context.assignment()).thenReturn(Set.of(PARTITION));
            storedOffset(25L);
            when(jedis.unwatch()).thenThrow(new JedisConnectionException("connection reset")).thenReturn("OK");

            List<SinkRecord> batch = List.of(record("team:1", CREDITS_JSON, 10L),
                    record("team:1", CREDITS_JSON, 20L));
            task.open(List.of(PARTITION));
            assertThrows(RetriableException.class, () -> task.put(batch));
            verify(context).offset(PARTITION, 26L);

            storedOffset(15L);
            task.put(batch);

            verify(context).offset(PARTITION, 16L);
            verify(transaction, never()).incrByFloat(anyString(), anyDouble());
        });
    }

    @Test
    void reconnect_nothingLost_theBatchIsAppliedWithoutReadingItAgain() {
        withConnectedRedis(mailSender -> {
            stubIncrByFloat(1.0);
            when(context.assignment()).thenReturn(Set.of(PARTITION));
            storedOffset(8L);
            when(transaction.exec()).thenThrow(new JedisConnectionException("connection reset"))
                    .thenReturn(List.of());

            List<SinkRecord> batch = List.of(record("team:1", CREDITS_JSON, 9L));
            assertThrows(RetriableException.class, () -> task.put(batch));
            task.put(batch);

            verify(context, never()).offset(any(TopicPartition.class), anyLong());
            verify(transaction, times(2)).incrByFloat(anyString(), anyDouble());
            verify(transaction, times(2)).hset(OFFSETS_KEY, "0", "9");
        });
    }

    @Test
    void assignedPartition_staysLeftOutWhenTheBatchIsRetried() {
        // Partition 0 is moved back, partition 1 is written and that fails. Connect
        // delivers the same batch again: applying partition 0's records now would
        // store an offset beyond the records still to be replayed.
        withConnectedRedis(mailSender -> {
            stubIncrByFloat(1.0);
            storedOffset(5L);
            when(transaction.exec()).thenReturn(null).thenReturn(List.of());

            List<SinkRecord> batch = List.of(record("team:1", CREDITS_JSON, 9L),
                    new SinkRecord("usage", 1, null, "team:2", null, CREDITS_JSON, 3L));

            task.open(List.of(PARTITION, new TopicPartition("usage", 1)));
            assertThrows(RetriableException.class, () -> task.put(batch));
            task.put(batch);

            verify(transaction, never()).incrByFloat(eq("team:1"), anyDouble());
            verify(transaction, never()).hset(OFFSETS_KEY, "0", "9");
            verify(transaction, times(2)).hset(OFFSETS_KEY, "1", "3");

            // After the seek, the partition is delivered from the stored offset.
            task.put(List.of(record("team:1", CREDITS_JSON, 7L), record("team:1", CREDITS_JSON, 9L)));
            verify(transaction, times(2)).incrByFloat("team:1", 1.0);
            verify(transaction).hset(OFFSETS_KEY, "0", "9");
        });
    }

    @Test
    void assignedPartition_redisUnavailable_isMovedOnTheRetry() {
        withConnectedRedis(mailSender -> {
            Map<String, String> offsets = new HashMap<>();
            offsets.put("0", "5");
            when(jedis.hgetAll(OFFSETS_KEY)).thenThrow(new JedisConnectionException("down")).thenReturn(offsets);

            task.open(List.of(PARTITION));
            assertThrows(RetriableException.class, () -> task.put(Collections.emptyList()));
            verify(context, never()).offset(any(TopicPartition.class), anyLong());

            task.put(Collections.emptyList());
            verify(context).offset(PARTITION, 6L);
        });
    }

    @Test
    void reconnect_movesTheAssignedPartitionsToTheOffsetsOfTheNewRedis() {
        // A failover promoted a replica that is behind: offset 8 was stored on the
        // old master, the new one holds 5.
        withConnectedRedis(mailSender -> {
            stubIncrByFloat(1.0);
            when(context.assignment()).thenReturn(Set.of(PARTITION));
            storedOffset(8L);
            when(transaction.exec()).thenThrow(new JedisConnectionException("connection reset"))
                    .thenReturn(List.of());

            List<SinkRecord> batch = List.of(record("team:1", CREDITS_JSON, 9L));
            assertThrows(RetriableException.class, () -> task.put(batch));
            verify(context, never()).offset(any(TopicPartition.class), anyLong());

            storedOffset(5L);
            task.put(batch);

            // Replayed from offset 6; the batch in hand is left out and read again.
            verify(context).offset(PARTITION, 6L);
            verify(transaction, times(1)).incrByFloat(anyString(), anyDouble());
        });
    }

    @Test
    void open_movesAReassignedPartitionAgain() {
        withConnectedRedis(mailSender -> {
            storedOffset(5L);

            task.open(List.of(PARTITION));
            task.put(Collections.emptyList());
            task.close(List.of(PARTITION));
            task.open(List.of(PARTITION));
            task.put(Collections.emptyList());

            verify(context, times(2)).offset(PARTITION, 6L);
        });
    }

    @Test
    void put_usesTheCoordinatesTheRecordWasReadFrom() {
        // A transformation renamed the topic; the offset belongs to the source topic.
        withConnectedRedis(mailSender -> {
            stubIncrByFloat(1.0);
            SinkRecord read = record("team:1", CREDITS_JSON, 7L);
            SinkRecord routed = read.newRecord("renamed", 3, null, "team:1", null, CREDITS_JSON, null);

            task.put(List.of(routed));

            verify(jedis).watch(OFFSETS_KEY);
            verify(transaction).hset(OFFSETS_KEY, "0", "7");
        });
    }

    @Test
    void put_offsetsNotStored_throwsDataException() {
        // The increments are applied; without the offsets they would be counted again.
        withConnectedRedis(mailSender -> {
            stubIncrByFloat(1.0);
            @SuppressWarnings("unchecked")
            Response<Long> rejected = mock(Response.class);
            when(rejected.get()).thenThrow(new JedisDataException("WRONGTYPE"));
            when(transaction.hset(anyString(), anyString(), anyString())).thenReturn(rejected);

            assertThrows(DataException.class, () -> task.put(List.of(record("team:1", CREDITS_JSON))));
        });
    }

    @Test
    void put_offsetsChangedConcurrently_throwsRetriable() {
        // EXEC returns null when a WATCHed key was modified: nothing was applied.
        withConnectedRedis(mailSender -> {
            stubIncrByFloat(1.0);
            when(transaction.exec()).thenReturn(null);

            assertThrows(RetriableException.class, () -> task.put(List.of(record("team:1", CREDITS_JSON))));
            verify(context).timeout(1000L);
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
