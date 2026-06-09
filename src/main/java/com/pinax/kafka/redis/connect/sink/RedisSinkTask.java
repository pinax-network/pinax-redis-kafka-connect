package com.pinax.kafka.redis.connect.sink;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.HashSet;

import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisAccessControlException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.Response;

public class RedisSinkTask extends SinkTask {
    private final Logger logger = LoggerFactory.getLogger(RedisSinkConnector.class);

    private Set<String> sentinels = new HashSet<String>();

    private JedisSentinelPool jedisSentinelPool = null;
    private Jedis jedis = null;
    private Pipeline jedisPipeline = null;

    private MailSender mailSender = null;

    private List<String> redisHosts;
    private String redisMaster;

    private void createRedisConnection() {
        // Let connection failures propagate. Callers decide how to react: startup
        // tolerates them (put() retries lazily) and put() maps them to a
        // RetriableException so Kafka Connect re-delivers the batch.
        jedisSentinelPool = new JedisSentinelPool(redisMaster, sentinels);
        jedis = jedisSentinelPool.getResource();
        jedisPipeline = jedis.pipelined();

        logger.info("Redis connection created");
    }

    private void closeRedisConnection() {
        try {
            if (jedisPipeline != null) {
                jedisPipeline.close();
            }
            if (jedis != null) {
                jedis.close();
            }
            if (jedisSentinelPool != null) {
                jedisSentinelPool.close();
            }
        } catch (Exception e) {
            logger.warn("Error while closing Redis connection", e);
        } finally {
            // Always clear references so a half-open/closed connection is never
            // reused; the next put() rebuilds from a clean state.
            jedisPipeline = null;
            jedis = null;
            jedisSentinelPool = null;

            logger.info("Redis connection closed");
        }
    }

    @Override
    public void start(Map<String, String> properties) {
        logger.info("Starting Redis sink task {}", properties);

        AbstractConfig config = new AbstractConfig(RedisSinkConfig.CONFIG_DEF, properties);

        // Prepare Redis connection
        redisHosts = config.getList(RedisSinkConfig.HOSTS);
        redisMaster = config.getString(RedisSinkConfig.MASTER);

        // Prepare email sender
        mailSender = new MailSender(
                config.getString(RedisSinkConfig.FROM),
                config.getString(RedisSinkConfig.MAILCHIMP_API_KEY),
                config.getString(RedisSinkConfig.TEMPLATE_SLUG));

        for (String redisHostPort : redisHosts) {
            sentinels.add(redisHostPort);
        }

        try {
            createRedisConnection();
        } catch (JedisAccessControlException e) {
            // Invalid credentials/ACLs are a permanent misconfiguration — fail fast
            // at startup instead of reporting a healthy task that can never write.
            logger.error("Redis access control error on startup", e);
            closeRedisConnection();
            throw new ConnectException("Redis access control error", e);
        } catch (JedisException e) {
            // A transient Redis outage at startup must not permanently fail the
            // task. Clean up any partial state; put() will (re)connect and retry.
            logger.warn("Could not connect to Redis on startup, will retry on first put", e);
            closeRedisConnection();
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }

        logger.debug("Received {} records from Connect", records.size());

        List<PendingWrite> pendingWrites = new ArrayList<PendingWrite>();
        // Assigned in the try below (every catch rethrows, so it is always set
        // before the mail-sending code reads it); no initializer needed.
        List<HttpPost> mailRequests;

        // Parse every record up front, before touching Redis, so a malformed payload
        // fails the batch without ever leaving half-built state on the pipeline.
        for (SinkRecord record : records) {
            logger.debug("Processing record: {}", record);

            String key = record.key() == null ? "" : record.key().toString();
            String value = record.value() == null ? "" : record.value().toString();

            try {
                JSONObject json = new JSONObject(value);
                double billedCredits = json.getDouble("billed_credits");
                long expireAtValue = json.getLong("expiration");

                pendingWrites.add(new PendingWrite(key, billedCredits, expireAtValue, json));
            } catch (JSONException e) {
                // Only JSON/field-shape problems are "bad data" — surface as a
                // non-retriable DataException. Anything unexpected propagates with
                // its real type rather than being mislabelled a parsing error.
                logger.error("Data or parsing error for record: {}", record, e);
                throw new DataException("Data or parsing error", e);
            }
        }

        // A pipeline only buffers commands locally; the connection is exercised by
        // sync() below. The whole batch shares one try/catch so any Redis failure
        // (connection, pool, sentinel) maps to a single RetriableException.
        //
        // Delivery is at-least-once: a retry re-applies the (non-idempotent)
        // increment, so credits may be counted more than once. That is accepted in
        // exchange for never missing a usage notification.
        try {
            if (jedisPipeline == null) {
                createRedisConnection();
            }

            for (PendingWrite write : pendingWrites) {
                write.response = jedisPipeline.incrByFloat(write.key, write.billedCredits);
                jedisPipeline.expireAt(write.key, write.expireAt);
            }

            jedisPipeline.sync();

            mailRequests = prepareMailRequests(pendingWrites);
        } catch (JedisAccessControlException e) {
            // Auth/ACL errors are permanent — retrying cannot fix them, so fail the
            // task loudly rather than hiding the misconfiguration behind retries.
            // (Subclass of JedisDataException, so it must be caught before it.)
            logger.error("Redis access control error", e);
            closeRedisConnection();
            throw new ConnectException("Redis access control error", e);
        } catch (JedisDataException e) {
            // Server rejected the command (e.g. WRONGTYPE, value not a valid float).
            // Permanent for this batch — retrying would loop forever and hide the
            // real data problem, so surface it as a non-retriable DataException.
            logger.error("Redis command/data error", e);
            closeRedisConnection();
            throw new DataException("Redis command/data error", e);
        } catch (JedisException e) {
            // Everything else under JedisException is infrastructure: connection
            // loss, pool exhaustion ("Could not get a resource from the pool") and
            // sentinel failover — all transient. Drop the connection and retry.
            logger.error("Redis connection error, will retry batch", e);
            closeRedisConnection();
            throw new RetriableException("Redis connection error", e);
        } catch (Exception e) {
            // Unexpected non-retriable failure while writing the batch to Redis.
            // (Malformed payloads are caught during parsing above, and a bad
            // notification field is skipped in prepareMailRequests, so this is a
            // genuine catch-all.) Drop the connection defensively so no buffered
            // command can ever be flushed by a later put() on this task.
            logger.error("Non-retriable error while writing batch to Redis", e);
            closeRedisConnection();
            throw new DataException("Non-retriable error while writing batch to Redis", e);
        }

        List<CompletableFuture<Void>> futures = new ArrayList<CompletableFuture<Void>>();

        if (mailRequests.size() > 0) {
            logger.info("Sending usage mail requests: {}", mailRequests.size());
        }

        mailRequests.forEach(mailRequest -> {
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    mailSender.SendUsageMailRequest(mailRequest);
                } catch (Exception e) {
                    logger.error("Failed to send mail", e);
                }
            });
            futures.add(future);
        });

        CompletableFuture<Void> allFutures = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        try {
            allFutures.get();
        } catch (Exception e) {
            logger.error("Failed to complete all mail requests", e);
        }
    }

    private List<HttpPost> prepareMailRequests(List<PendingWrite> pendingWrites) {
        List<HttpPost> mailRequests = new ArrayList<HttpPost>();

        for (PendingWrite write : pendingWrites) {
            // Read the pipelined result outside the try below: Jedis surfaces a
            // per-command server error (e.g. WRONGTYPE, "value is not a valid float")
            // here at get(), not at sync(). Letting it propagate routes it to put()'s
            // JedisDataException handler instead of silently dropping a failed write.
            double newBilledCredits = write.response.get();
            double oldBilledCredits = newBilledCredits - write.billedCredits;

            try {
                JSONObject json = write.json;
                int includedCredits = json.getInt("included_credits");

                // Any team with an included-credit allowance to measure against receives
                // usage notifications.
                if (includedCredits <= 0) {
                    continue;
                }

                List<Double> creditThresholds = new ArrayList<Double>();
                creditThresholds.add(includedCredits * 0.50);
                creditThresholds.add(includedCredits * 0.75);
                creditThresholds.add(includedCredits * 0.90);
                creditThresholds.add(includedCredits * 1.00);
                creditThresholds.add(includedCredits * 1.50);
                creditThresholds.add(includedCredits * 2.00);

                // Format to currency and remove the currency symbol.
                DecimalFormat formatter = (DecimalFormat) NumberFormat.getCurrencyInstance(Locale.US);
                DecimalFormatSymbols symbols = formatter.getDecimalFormatSymbols();
                symbols.setCurrencySymbol("");
                formatter.setDecimalFormatSymbols(symbols);

                // A single batch can leap past several milestones at once; notify on the
                // highest one crossed (the most significant), so walk high-to-low and stop
                // at the first match.
                for (int i = creditThresholds.size() - 1; i >= 0; i--) {
                    double creditThreshold = creditThresholds.get(i);
                    if (oldBilledCredits < creditThreshold && newBilledCredits >= creditThreshold) {
                        String newBilledCreditsString = formatter.format(newBilledCredits / 100);
                        String includedCreditsString = formatter.format(includedCredits / 100);

                        String teamBillingEmail = json.getString("team_billing_email");
                        String teamName = json.getString("team_name");
                        String teamPlan = json.getString("team_plan");

                        MailContent mailContent = new MailContent(teamName, teamPlan, newBilledCreditsString,
                                includedCreditsString);
                        mailRequests.add(mailSender.CreateUsageMailRequest(teamBillingEmail,
                                "An Update on your Monthly Usage", mailContent)); // TODO: Change the mail subject
                        break;
                    }
                }
            } catch (JSONException e) {
                // Only a malformed/missing notification field is skippable — the Redis
                // write already succeeded, so don't fail the batch over a bad email.
                // Redis/infra errors are NOT caught here; they propagate to put().
                logger.error("Skipping usage notification for malformed record: key={}", write.key, e);
            }
        }

        return mailRequests;
    }

    @Override
    public void stop() {
        logger.info("Stopping Redis sink task");
        closeRedisConnection();
        if (mailSender != null) {
            mailSender.close();
        }
    }

    @Override
    public String version() {
        return RedisSinkConnector.VERSION;
    }

    /**
     * A parsed sink record awaiting its write. {@code response} holds the new
     * billed-credits value once the pipeline is synced.
     */
    private static final class PendingWrite {
        final String key;
        final double billedCredits;
        final long expireAt;
        final JSONObject json;
        Response<Double> response;

        PendingWrite(String key, double billedCredits, long expireAt, JSONObject json) {
            this.key = key;
            this.billedCredits = billedCredits;
            this.expireAt = expireAt;
            this.json = json;
        }
    }
}
