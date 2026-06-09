package com.pinax.kafka.redis.connect.sink;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
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
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisAccessControlException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

/**
 * Unit tests for {@link RedisSinkTask}. The task constructs its
 * {@link JedisSentinelPool} and {@link MailSender} directly, so we intercept
 * those constructions with Mockito (no real Redis or HTTP) and drive behaviour
 * through the mocked {@link Pipeline}. End-to-end Redis semantics are covered
 * separately by {@code RedisSinkIT}.
 */
class RedisSinkTaskTest {

    private Jedis jedis;
    private Pipeline pipeline;
    private RedisSinkTask task;

    @BeforeEach
    void setUp() {
        jedis = mock(Jedis.class);
        pipeline = mock(Pipeline.class);
        when(jedis.pipelined()).thenReturn(pipeline);
        task = new RedisSinkTask();
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
        return new SinkRecord("usage", 0, null, key, null, json, 0L);
    }

    @SuppressWarnings("unchecked")
    private Response<Double> stubIncrByFloat(double newValue) {
        Response<Double> response = mock(Response.class);
        when(response.get()).thenReturn(newValue);
        when(pipeline.incrByFloat(anyString(), anyDouble())).thenReturn(response);
        when(pipeline.expireAt(anyString(), anyLong())).thenReturn(mock(Response.class));
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
    void put_syncConnectionError_throwsRetriable() {
        withConnectedRedis(mailSender -> {
            stubIncrByFloat(1.0);
            doThrow(new JedisConnectionException("connection reset")).when(pipeline).sync();

            List<SinkRecord> records = List.of(
                    record("team:1", "{\"billed_credits\":1.0,\"expiration\":9999999999}"));
            assertThrows(RetriableException.class, () -> task.put(records));
        });
    }

    @Test
    void put_syncAccessControlError_throwsConnectException() {
        withConnectedRedis(mailSender -> {
            stubIncrByFloat(1.0);
            doThrow(new JedisAccessControlException("NOPERM")).when(pipeline).sync();

            List<SinkRecord> records = List.of(
                    record("team:1", "{\"billed_credits\":1.0,\"expiration\":9999999999}"));
            assertThrows(ConnectException.class, () -> task.put(records));
        });
    }

    @Test
    void put_syncDataError_throwsDataException() {
        withConnectedRedis(mailSender -> {
            stubIncrByFloat(1.0);
            doThrow(new JedisDataException("ERR value is not a valid float")).when(pipeline).sync();

            List<SinkRecord> records = List.of(
                    record("team:1", "{\"billed_credits\":1.0,\"expiration\":9999999999}"));
            assertThrows(DataException.class, () -> task.put(records));
        });
    }

    @Test
    void put_perCommandErrorSurfacedAtGet_throwsDataException() {
        // Jedis raises per-command errors (WRONGTYPE, bad float) at Response.get(),
        // not at sync(). prepareMailRequests reads get() outside its catch so the
        // error reaches put()'s JedisDataException handler -> DataException.
        withConnectedRedis(mailSender -> {
            @SuppressWarnings("unchecked")
            Response<Double> response = mock(Response.class);
            when(response.get()).thenThrow(new JedisDataException("WRONGTYPE"));
            when(pipeline.incrByFloat(anyString(), anyDouble())).thenReturn(response);
            when(pipeline.expireAt(anyString(), anyLong())).thenReturn(mock(Response.class));

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
