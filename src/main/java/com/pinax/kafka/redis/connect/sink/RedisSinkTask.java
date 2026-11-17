package com.pinax.kafka.redis.connect.sink;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.HashSet;

import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
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

    // Partitions assigned to this task whose consumer has not been moved to the offset
    // stored in Redis yet, see seekToStoredOffsets().
    private final Set<TopicPartition> unseekedPartitions = new HashSet<TopicPartition>();

    // Partitions a seek was requested for that Connect has not carried out yet. The
    // batch in hand was read from the old position, so their records are left out.
    private final Set<TopicPartition> seekingPartitions = new HashSet<TopicPartition>();

    // Errors Redis answers with while it cannot take writes for the moment: loading
    // its dataset, blocked by a script, demoted to a replica, unable to persist, out
    // of memory, or without enough replicas. Retried, they pass.
    private static final String[] TRANSIENT_ERRORS = {
        "LOADING", "BUSY", "READONLY", "MISCONF", "OOM", "MASTERDOWN", "NOREPLICAS", "TRYAGAIN", "CLUSTERDOWN" };
    private static final String EXECABORT_CAUSE = "EXECABORT Transaction discarded because of: ";

    static final String REJECTED_COMMANDS_METRIC = "rejected-commands-total";

    // Counts the commands of committed batches that Redis rejected. Null when the
    // Connect worker does not offer plugin metrics.
    private Sensor rejectedCommands = null;

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
        unseekedPartitions.clear();
        seekingPartitions.clear();

        // Prepare email sender
        mailSender = new MailSender(
                config.getString(RedisSinkConfig.FROM),
                config.getString(RedisSinkConfig.MAILCHIMP_API_KEY),
                config.getString(RedisSinkConfig.TEMPLATE_SLUG));

        for (String redisHostPort : redisHosts) {
            sentinels.add(redisHostPort);
        }

        registerMetrics();

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

    private void registerMetrics() {
        rejectedCommands = null;
        try {
            PluginMetrics metrics = context.pluginMetrics();
            if (metrics == null) {
                return;
            }
            MetricName name = metrics.metricName(REJECTED_COMMANDS_METRIC,
                    "Commands of committed batches that Redis rejected (wrong key type, value not a number, "
                            + "invalid expiration). The record's increment or expiration was not applied.",
                    new LinkedHashMap<String, String>());
            rejectedCommands = metrics.addSensor("rejected-commands");
            rejectedCommands.add(name, new CumulativeSum());
        } catch (NoSuchMethodError | NoClassDefFoundError e) {
            // Plugin metrics came with Kafka Connect 4.1.
            logger.warn("This Connect worker has no plugin metrics, {} is only logged", REJECTED_COMMANDS_METRIC);
        }
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        // Newly assigned partitions start at the offsets Kafka has committed for the
        // consumer group; the next put() moves them to the ones in Redis. Not done
        // here: an unreachable Redis would fail the task instead of being retried.
        unseekedPartitions.addAll(partitions);
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        unseekedPartitions.removeAll(partitions);
        seekingPartitions.removeAll(partitions);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        // Connect calls put() after every poll, also without records. That is what
        // moves a partition to its stored offset when nothing new arrives on it.
        if (records.isEmpty() && unseekedPartitions.isEmpty()) {
            seekingPartitions.clear();
            return;
        }

        List<HttpPost> mailRequests = writeToRedis(records);

        // Connect carries out the requested seeks before it polls again. Only a
        // batch it delivers again after a RetriableException, which never gets
        // here, is still the one read from the old positions.
        seekingPartitions.clear();

        // Notifications are at-most-once. The batch is committed in Redis by now, so
        // if the task stops before or while sending, or a send fails, the batch is
        // skipped as already applied when it is delivered again and these mails are
        // not sent. Accepted: the team is notified again at the next milestone.
        sendMails(mailRequests);
    }

    private List<HttpPost> writeToRedis(Collection<SinkRecord> records) {

        logger.debug("Received {} records from Connect", records.size());

        List<PendingWrite> pendingWrites = new ArrayList<PendingWrite>();
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

                // Where the consumer read the record, not where a transformation
                // (a topic router, for one) says it is: these are the coordinates
                // the consumer is assigned and can be moved to.
                pendingWrites.add(new PendingWrite(
                        new TopicPartition(record.originalTopic(), record.originalKafkaPartition()),
                        record.originalKafkaOffset(), key, billedCredits, expireAtValue, json));
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
                // The Redis behind a new connection may be another one than before:
                // a replica promoted in a failover that had not received the last
                // writes, or a restored snapshot. Its counters and offsets belong
                // together, so continue every partition from the offsets it holds.
                unseekedPartitions.addAll(context.assignment());
            }

            seekToStoredOffsets(pendingWrites);

            Map<TopicPartition, Long> storedOffsets = watchStoredOffsets(pendingWrites);
            pendingWrites = notYetApplied(pendingWrites, storedOffsets);

            if (pendingWrites.isEmpty()) {
                jedis.unwatch();
                return new ArrayList<HttpPost>();
            }

            Map<TopicPartition, Long> appliedOffsets = new HashMap<TopicPartition, Long>();
            List<Response<Long>> offsetResponses = new ArrayList<Response<Long>>();
            Transaction transaction = jedis.multi();

            for (PendingWrite write : pendingWrites) {
                write.response = transaction.incrByFloat(write.key, write.billedCredits);
                write.expireResponse = transaction.expireAt(write.key, write.expireAt);
                appliedOffsets.merge(write.partition, write.offset, Math::max);
            }
            for (Map.Entry<TopicPartition, Long> applied : appliedOffsets.entrySet()) {
                offsetResponses.add(transaction.hset(offsetsKey(applied.getKey().topic()),
                        Integer.toString(applied.getKey().partition()), Long.toString(applied.getValue())));
            }

            List<Object> results = transaction.exec();
            if (results == null) {
                // Nothing was applied. Not an error of this batch, so retry it.
                logger.warn("Offsets in Redis changed while writing the batch, will retry it");
                throw new RetriableException("Offsets in Redis changed while writing the batch");
            }
            // The transaction is applied, but Redis does not roll it back when it
            // rejects one of its commands: that command's Response throws at get().
            //
            // Offsets that were not stored fail the task. The increments are applied,
            // and carrying on would count them again after the next restart.
            for (Response<Long> offsetResponse : offsetResponses) {
                try {
                    offsetResponse.get();
                } catch (JedisDataException e) {
                    throw new DataException("Batch applied to Redis but its offsets were not stored", e);
                }
            }
            // A rejected increment or expiration concerns one record only (its key
            // holds another type or no number, its expiration is out of range). The
            // rest of the batch is applied and nothing can be retried, so it is
            // reported and the task carries on.
            for (PendingWrite write : pendingWrites) {
                try {
                    write.newBilledCredits = write.response.get();
                } catch (JedisDataException e) {
                    write.incrementRejected = true;
                    reportRejected("INCRBYFLOAT", write, e);
                }
                try {
                    write.expireResponse.get();
                } catch (JedisDataException e) {
                    reportRejected("EXPIREAT", write, e);
                }
            }

            return prepareMailRequests(pendingWrites);
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
            if (isTransient(e)) {
                // Redis refused the read or the whole transaction because it cannot
                // take writes right now. Nothing was applied, so retry the batch. On
                // a new connection: after a failover the sentinels name another
                // master.
                logger.warn("Redis is temporarily not accepting writes, will retry batch", e);
                closeRedisConnection();
                throw new RetriableException("Redis is temporarily not accepting writes", e);
            }
            // Server refused a read or the whole transaction for a reason retrying
            // cannot fix (e.g. the offset hash holds another type). Surface it as a
            // non-retriable DataException instead of looping on it.
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
    }

    private void reportRejected(String command, PendingWrite write, JedisDataException e) {
        logger.error("Redis rejected {} for key {} (record {} offset {}), the rest of the batch is applied: {}",
                command, write.key, write.partition, write.offset, e.getMessage());
        if (rejectedCommands != null) {
            rejectedCommands.record();
        }
    }

    /**
     * Whether Redis refused a command, or a transaction as a whole, for a reason that
     * passes. A discarded transaction (EXECABORT) names the reason in its message or,
     * when a command was refused while it was queued, carries the refusals as
     * suppressed exceptions; it is transient when all of them are.
     */
    static boolean isTransient(JedisDataException e) {
        String message = e.getMessage() == null ? "" : e.getMessage();
        if (message.startsWith(EXECABORT_CAUSE)) {
            message = message.substring(EXECABORT_CAUSE.length());
        }
        if (isTransientError(message)) {
            return true;
        }

        Throwable[] refusals = e.getSuppressed();
        for (Throwable refusal : refusals) {
            if (!(refusal instanceof JedisDataException) || !isTransientError(refusal.getMessage())) {
                return false;
            }
        }
        return refusals.length > 0;
    }

    private static boolean isTransientError(String message) {
        if (message == null) {
            return false;
        }
        for (String error : TRANSIENT_ERRORS) {
            if (message.equals(error) || message.startsWith(error + " ")) {
                return true;
            }
        }
        return false;
    }

    private void sendMails(List<HttpPost> mailRequests) {
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
            readStoredOffsets(topic, storedOffsets);
        }
        return storedOffsets;
    }

    private void readStoredOffsets(String topic, Map<TopicPartition, Long> storedOffsets) {
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

    /**
     * Makes Redis, not the consumer group, decide where a partition continues after
     * it was assigned to this task: the consumer is moved to the record following
     * the stored offset. That is what replays the missing records after Redis was
     * restored from a snapshot, whose counters and offsets are both from the time of
     * the snapshot. It does not wait for a record, so a partition nothing is produced
     * to is replayed as well.
     *
     * <p>A partition without a stored offset (first start with this version, or an
     * empty Redis) stays where the consumer group has it.
     */
    private void seekToStoredOffsets(List<PendingWrite> pendingWrites) {
        if (unseekedPartitions.isEmpty()) {
            return;
        }

        // Where the batch in hand starts, to tell how far back a partition is moved.
        Map<TopicPartition, Long> firstInHand = new HashMap<TopicPartition, Long>();
        for (PendingWrite write : pendingWrites) {
            firstInHand.merge(write.partition, write.offset, Math::min);
        }

        Set<String> topics = new HashSet<String>();
        for (TopicPartition partition : unseekedPartitions) {
            topics.add(partition.topic());
        }
        // Read everything first: a failure leaves the partitions to the next put().
        Map<TopicPartition, Long> storedOffsets = new HashMap<TopicPartition, Long>();
        for (String topic : topics) {
            readStoredOffsets(topic, storedOffsets);
        }

        Set<String> topicsWithOffsets = new HashSet<String>();
        for (TopicPartition stored : storedOffsets.keySet()) {
            topicsWithOffsets.add(stored.topic());
        }

        for (TopicPartition partition : unseekedPartitions) {
            Long storedOffset = storedOffsets.get(partition);
            if (storedOffset != null) {
                Long first = firstInHand.get(partition);
                if (first != null && first > storedOffset + 1) {
                    // Expected after Redis was restored or lost writes in a failover.
                    // Otherwise the offsets in Redis are stale (written by this
                    // version, then left behind by an older one) and these records
                    // are counted a second time.
                    logger.warn("Moving {} back to offset {}, the record after the last one applied to Redis: "
                            + "{} records the consumer group had already passed are applied again",
                            partition, storedOffset + 1, first - storedOffset - 1);
                } else {
                    logger.info("Continuing {} at offset {}, the record after the last one applied to Redis",
                            partition, storedOffset + 1);
                }
                context.offset(partition, storedOffset + 1);
                seekingPartitions.add(partition);
            } else if (topicsWithOffsets.contains(partition.topic())) {
                // Gets its entry with the first record applied. A snapshot taken
                // before that restores this partition to wherever the consumer group
                // is, not to the time of the snapshot.
                logger.warn("No offset stored in Redis for {} yet although other partitions of the topic have "
                        + "one: it follows the consumer group until its first record is applied", partition);
            }
        }
        unseekedPartitions.clear();
    }

    private List<PendingWrite> notYetApplied(List<PendingWrite> pendingWrites,
            Map<TopicPartition, Long> storedOffsets) {
        List<PendingWrite> remaining = new ArrayList<PendingWrite>();
        int alreadyApplied = 0;

        for (PendingWrite write : pendingWrites) {
            // Delivered again from the stored offset once the seek is carried out.
            if (seekingPartitions.contains(write.partition)) {
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
            // Nothing was added for this record, so it crossed no milestone.
            if (write.incrementRejected) {
                continue;
            }
            double newBilledCredits = write.newBilledCredits;
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
     * A parsed sink record awaiting its write. The responses hold the results of its
     * commands once the transaction is executed.
     */
    private static final class PendingWrite {
        final TopicPartition partition;
        final long offset;
        final String key;
        final double billedCredits;
        final long expireAt;
        final JSONObject json;
        Response<Double> response;
        Response<Long> expireResponse;
        double newBilledCredits;
        boolean incrementRejected;

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
