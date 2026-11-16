package com.pinax.kafka.redis.connect.sink;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.HashSet;

import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.kafka.common.TopicPartition;
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
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisAccessControlException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.Response;

public class RedisSinkTask extends SinkTask {
    private final Logger logger = LoggerFactory.getLogger(RedisSinkConnector.class);

    private Set<String> sentinels = new HashSet<String>();

    private JedisSentinelPool jedisSentinelPool = null;
    private Jedis jedis = null;

    private MailSender mailSender = null;

    private List<String> redisHosts;
    private String redisMaster;
    private String offsetsKey;

    // Partitions whose consumer position has been checked against the offset stored
    // in Redis since they were assigned to this task, see rewindToStoredOffsets().
    private final Set<TopicPartition> checkedPartitions = new HashSet<TopicPartition>();

    private void createRedisConnection() {
        // Let connection failures propagate. Callers decide how to react: startup
        // tolerates them (put() retries lazily) and put() maps them to a
        // RetriableException so Kafka Connect re-delivers the batch.
        jedisSentinelPool = new JedisSentinelPool(redisMaster, sentinels);
        jedis = jedisSentinelPool.getResource();

        logger.info("Redis connection created");
    }

    private void closeRedisConnection() {
        try {
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
            // reused; the next put() rebuilds from a clean state. Closing the
            // connection also discards a pending WATCH or MULTI.
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
        offsetsKey = config.getString(RedisSinkConfig.OFFSETS_KEY);
        checkedPartitions.clear();

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
    public void open(Collection<TopicPartition> partitions) {
        // Newly assigned partitions start at the offsets Kafka has committed for the
        // consumer group; the first put() compares them with the ones in Redis.
        checkedPartitions.removeAll(partitions);
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

                pendingWrites.add(new PendingWrite(
                        new TopicPartition(record.topic(), record.kafkaPartition()), record.kafkaOffset(),
                        key, billedCredits, expireAtValue, json));
            } catch (JSONException e) {
                // Only JSON/field-shape problems are "bad data" — surface as a
                // non-retriable DataException. Anything unexpected propagates with
                // its real type rather than being mislabelled a parsing error.
                logger.error("Data or parsing error for record: {}", record, e);
                throw new DataException("Data or parsing error", e);
            }
        }

        // Each batch is one Redis transaction (MULTI/EXEC) that applies the increments
        // and stores, per partition, the offset of the last record applied. Counters
        // and offsets therefore always belong together, also in a snapshot or on a
        // replica, and a record at or below the stored offset is known to be counted
        // already: a batch Connect delivers again after a failure, a restart or a
        // rewind of the consumer group is skipped instead of being counted twice.
        //
        // The offset keys are WATCHed, so the transaction is refused when another task
        // (one that lost its partitions but has not noticed yet) stored offsets in
        // between; the batch is then retried against the new offsets.
        //
        // The whole batch shares one try/catch so any Redis failure (connection, pool,
        // sentinel) maps to a single RetriableException.
        try {
            if (jedis == null) {
                createRedisConnection();
            }

            Map<TopicPartition, Long> storedOffsets = watchStoredOffsets(pendingWrites);
            pendingWrites = notYetApplied(pendingWrites, rewindToStoredOffsets(pendingWrites, storedOffsets),
                    storedOffsets);

            if (pendingWrites.isEmpty()) {
                jedis.unwatch();
                return;
            }

            Map<TopicPartition, Long> appliedOffsets = new HashMap<TopicPartition, Long>();
            Transaction transaction = jedis.multi();

            for (PendingWrite write : pendingWrites) {
                write.response = transaction.incrByFloat(write.key, write.billedCredits);
                transaction.expireAt(write.key, write.expireAt);
                appliedOffsets.merge(write.partition, write.offset, Math::max);
            }
            for (Map.Entry<TopicPartition, Long> applied : appliedOffsets.entrySet()) {
                transaction.hset(offsetsKey(applied.getKey().topic()),
                        Integer.toString(applied.getKey().partition()), Long.toString(applied.getValue()));
            }

            if (transaction.exec() == null) {
                // Nothing was applied. Not an error of this batch, so retry it.
                logger.warn("Offsets in Redis changed while writing the batch, will retry it");
                throw new RetriableException("Offsets in Redis changed while writing the batch");
            }

            mailRequests = prepareMailRequests(pendingWrites);
        } catch (RetriableException e) {
            // The refused transaction above; the connection is still good.
            throw e;
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
            // Redis does not roll a transaction back: the other commands of the batch,
            // its offsets included, are applied, so the batch is not counted again
            // once the task is restarted.
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
            // genuine catch-all.) Drop the connection defensively so no queued
            // command can ever be executed by a later put() on this task.
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

    private String offsetsKey(String topic) {
        return offsetsKey + ":" + topic;
    }

    /**
     * WATCHes the offset hash of every topic in the batch and returns the offsets
     * stored in them: per partition, the offset of the last record applied to Redis.
     */
    private Map<TopicPartition, Long> watchStoredOffsets(List<PendingWrite> pendingWrites) {
        Set<String> topics = new HashSet<String>();
        for (PendingWrite write : pendingWrites) {
            topics.add(write.partition.topic());
        }

        Map<TopicPartition, Long> storedOffsets = new HashMap<TopicPartition, Long>();
        for (String topic : topics) {
            jedis.watch(offsetsKey(topic));
            for (Map.Entry<String, String> stored : jedis.hgetAll(offsetsKey(topic)).entrySet()) {
                try {
                    storedOffsets.put(new TopicPartition(topic, Integer.parseInt(stored.getKey())),
                            Long.parseLong(stored.getValue()));
                } catch (NumberFormatException e) {
                    throw new DataException("Invalid offset in Redis hash " + offsetsKey(topic) + ": "
                            + stored.getKey() + " = " + stored.getValue(), e);
                }
            }
        }
        return storedOffsets;
    }

    /**
     * Makes Redis, not the consumer group, decide where a partition continues after
     * it was assigned to this task. When the first batch of a partition starts
     * beyond the record following the stored offset, the consumer is rewound to that
     * record and the partition is left out of this batch; Connect delivers it again
     * from there. That is what replays the missing records after Redis was restored
     * from a snapshot: its counters and its offsets are both from the time of the
     * snapshot.
     *
     * <p>Offsets are not contiguous (a transactional producer leaves a marker after
     * every transaction), so a gap alone does not mean records are missing. The
     * rewind is harmless then: the same records arrive again and are applied.
     *
     * @return the partitions that were rewound
     */
    private Set<TopicPartition> rewindToStoredOffsets(List<PendingWrite> pendingWrites,
            Map<TopicPartition, Long> storedOffsets) {
        Set<TopicPartition> rewound = new HashSet<TopicPartition>();

        // Records arrive in offset order per partition, so the first one seen is the
        // lowest of the batch.
        for (PendingWrite write : pendingWrites) {
            if (!checkedPartitions.add(write.partition)) {
                continue;
            }

            Long storedOffset = storedOffsets.get(write.partition);
            if (storedOffset != null && write.offset > storedOffset + 1) {
                logger.info("Rewinding {} from offset {} to {}, the record after the last one applied to Redis",
                        write.partition, write.offset, storedOffset + 1);
                context.offset(write.partition, storedOffset + 1);
                rewound.add(write.partition);
            }
        }
        return rewound;
    }

    private List<PendingWrite> notYetApplied(List<PendingWrite> pendingWrites, Set<TopicPartition> rewound,
            Map<TopicPartition, Long> storedOffsets) {
        List<PendingWrite> remaining = new ArrayList<PendingWrite>();
        int alreadyApplied = 0;

        for (PendingWrite write : pendingWrites) {
            if (rewound.contains(write.partition)) {
                continue;
            }

            Long storedOffset = storedOffsets.get(write.partition);
            if (storedOffset != null && write.offset <= storedOffset) {
                alreadyApplied++;
                continue;
            }
            remaining.add(write);
        }

        if (alreadyApplied > 0) {
            logger.info("Skipping {} records that were already applied to Redis", alreadyApplied);
        }
        return remaining;
    }

    private List<HttpPost> prepareMailRequests(List<PendingWrite> pendingWrites) {
        List<HttpPost> mailRequests = new ArrayList<HttpPost>();

        for (PendingWrite write : pendingWrites) {
            // Read the result outside the try below: Jedis surfaces a per-command
            // server error (e.g. WRONGTYPE, "value is not a valid float") here at
            // get(), not at exec(). Letting it propagate routes it to put()'s
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
     * billed-credits value once the transaction is executed.
     */
    private static final class PendingWrite {
        final TopicPartition partition;
        final long offset;
        final String key;
        final double billedCredits;
        final long expireAt;
        final JSONObject json;
        Response<Double> response;

        PendingWrite(TopicPartition partition, long offset, String key, double billedCredits, long expireAt,
                JSONObject json) {
            this.partition = partition;
            this.offset = offset;
            this.key = key;
            this.billedCredits = billedCredits;
            this.expireAt = expireAt;
            this.json = json;
        }
    }
}
