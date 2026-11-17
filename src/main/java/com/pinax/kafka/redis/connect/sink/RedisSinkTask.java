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

    private String redisMaster;
    private String offsetsKey;
    private long retryBackoffMs;
    private long conflictRetryBackoffMs;
    private int maxRetries;

    // Attempts that ended in a retry since the last batch that went through.
    private int retries = 0;

    // Partitions assigned to this task whose consumer has not been moved to the offset
    // stored in Redis yet, see seekToStoredOffsets().
    private final Set<TopicPartition> unseekedPartitions = new HashSet<TopicPartition>();

    // Partitions a seek was requested for that Connect has not carried out yet. The
    // batch in hand was read from the old position, so their records are left out.
    private final Set<TopicPartition> seekingPartitions = new HashSet<TopicPartition>();

    // Where each of those seeks went, until the partition delivers its next record,
    // see checkSoughtOffsets().
    private final Map<TopicPartition, Long> soughtOffsets = new HashMap<TopicPartition, Long>();

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
        List<String> redisHosts = config.getList(RedisSinkConfig.HOSTS);
        redisMaster = config.getString(RedisSinkConfig.MASTER);
        offsetsKey = config.getString(RedisSinkConfig.OFFSETS_KEY);
        retryBackoffMs = config.getLong(RedisSinkConfig.RETRY_BACKOFF_MS);
        conflictRetryBackoffMs = config.getLong(RedisSinkConfig.CONFLICT_RETRY_BACKOFF_MS);
        maxRetries = config.getInt(RedisSinkConfig.MAX_RETRIES);
        retries = 0;
        unseekedPartitions.clear();
        seekingPartitions.clear();
        soughtOffsets.clear();

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
        // consumer group; the next put() moves them to the ones in Redis. Not done
        // here: an unreachable Redis would fail the task instead of being retried.
        unseekedPartitions.addAll(partitions);
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        unseekedPartitions.removeAll(partitions);
        seekingPartitions.removeAll(partitions);
        soughtOffsets.keySet().removeAll(partitions);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        // Connect calls put() after every poll, also without records. That is what
        // moves a partition to its stored offset when nothing new arrives on it.
        if (records.isEmpty() && unseekedPartitions.isEmpty()) {
            // The batch that was being retried is gone (its partitions were revoked).
            seekingPartitions.clear();
            retries = 0;
            return;
        }

        BatchResult result = writeToRedis(records);
        retries = 0;

        // Connect carries out the requested seeks before it polls again. Only a
        // batch it delivers again after a RetriableException, which never gets
        // here, is still the one read from the old positions.
        seekingPartitions.clear();

        // Notifications are at-most-once. The batch is committed in Redis by now, so
        // if the task stops before or while sending, or a send fails, the batch is
        // skipped as already applied when it is delivered again and these mails are
        // not sent. Accepted: the team is notified again at the next milestone.
        sendMails(result.mailRequests);

        // What could be applied is, and its notifications are sent. Now fail the task
        // over the record that could not: a value that cannot be parsed, or a command
        // Redis rejected.
        if (result.failure != null) {
            throw result.failure;
        }
    }

    private BatchResult writeToRedis(Collection<SinkRecord> records) {

        logger.debug("Received {} records from Connect", records.size());

        List<PendingWrite> pendingWrites = new ArrayList<PendingWrite>();
        for (SinkRecord record : records) {
            logger.debug("Processing record: {}", record);

            // Where the consumer read the record, not where a transformation
            // (a topic router, for one) says it is: these are the coordinates
            // the consumer is assigned and can be moved to.
            pendingWrites.add(new PendingWrite(
                    new TopicPartition(record.originalTopic(), record.originalKafkaPartition()),
                    record.originalKafkaOffset(), record));
        }

        // Each batch is one Redis transaction (MULTI/EXEC) that applies the increments
        // and stores, per partition, the offset of the last record applied. Counters
        // and offsets therefore always belong together, also in a snapshot or on a
        // replica, and a record at or below the stored offset is known to be counted
        // already: a batch Connect delivers again after a failure, a restart or a
        // rewind of the consumer group is skipped instead of being counted twice.
        //
        // The offset keys are WATCHed, so the transaction is refused when another task
        // (one that was replaced but has not noticed yet) stored offsets in between;
        // the batch is then retried against the new offsets.
        //
        // The whole batch shares one try/catch so any Redis failure (connection, pool,
        // sentinel) maps to a single RetriableException. Each sets the time after
        // which Connect delivers the batch again; without it that is only when its
        // next poll returns, up to offset.flush.interval.ms later.
        try {
            if (jedis == null) {
                createRedisConnection();
                // The Redis behind a new connection may be another one than before:
                // a replica promoted in a failover that had not received the last
                // writes, or a restored snapshot. Its counters and offsets belong
                // together, so continue every partition from the offsets it holds.
                unseekedPartitions.addAll(context.assignment());
            }

            // Where the batch in hand starts and ends, per partition.
            Map<TopicPartition, Long> firstInHand = new HashMap<TopicPartition, Long>();
            Map<TopicPartition, Long> lastInHand = new HashMap<TopicPartition, Long>();
            for (PendingWrite write : pendingWrites) {
                firstInHand.merge(write.partition, write.offset, Math::min);
                lastInHand.merge(write.partition, write.offset, Math::max);
            }

            // One read serves both: where to move newly assigned partitions to, and
            // which records of the batch are applied already.
            boolean watching = !pendingWrites.isEmpty();
            Map<TopicPartition, Long> storedOffsets = readStoredOffsets(pendingWrites, watching);
            seekToStoredOffsets(firstInHand, lastInHand, storedOffsets);
            checkSoughtOffsets(firstInHand);
            pendingWrites = notYetApplied(pendingWrites, storedOffsets);

            // Only what is left is parsed: a record at or below the stored offset is
            // not, which is what gets the task past one that cannot be parsed once
            // the stored offset was moved to it by hand. Of a partition with such a
            // record, the records before it are still applied, so that the stored
            // offset ends up right before it and moving it skips nothing else.
            List<PendingWrite> parsed = new ArrayList<PendingWrite>();
            DataException failure = parse(pendingWrites, parsed);
            pendingWrites = parsed;

            if (pendingWrites.isEmpty()) {
                if (failure != null) {
                    throw failure;
                }
                if (watching) {
                    jedis.unwatch();
                }
                return new BatchResult(new ArrayList<HttpPost>(), null);
            }

            Map<TopicPartition, Long> appliedOffsets = new HashMap<TopicPartition, Long>();
            List<Response<Long>> offsetResponses = new ArrayList<Response<Long>>();
            Transaction transaction = jedis.multi();

            for (PendingWrite write : pendingWrites) {
                appliedOffsets.merge(write.partition, write.offset, Math::max);
                write.response = transaction.incrByFloat(write.key, write.billedCredits);
                write.expireResponse = transaction.expireAt(write.key, write.expireAt);
            }
            for (Map.Entry<TopicPartition, Long> applied : appliedOffsets.entrySet()) {
                offsetResponses.add(transaction.hset(offsetsKey(applied.getKey().topic()),
                        Integer.toString(applied.getKey().partition()), Long.toString(applied.getValue())));
            }

            List<Object> results = transaction.exec();
            if (results == null) {
                // Nothing was applied. Not an error of this batch, so retry it.
                logger.warn("Offsets in Redis changed while writing the batch, will retry it");
                throw retry("Offsets in Redis changed while writing the batch", null, conflictRetryBackoffMs);
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
            // rest of the batch is applied and nothing can be retried. The task fails
            // over it once the notifications of the other records are sent; after a
            // restart it continues behind the batch.
            int rejected = 0;
            for (PendingWrite write : pendingWrites) {
                try {
                    write.newBilledCredits = write.response.get();
                } catch (JedisDataException e) {
                    write.incrementRejected = true;
                    rejected++;
                    logRejected("INCRBYFLOAT", write, e);
                }
                try {
                    write.expireResponse.get();
                } catch (JedisDataException e) {
                    rejected++;
                    logRejected("EXPIREAT", write, e);
                }
            }
            if (rejected > 0 && failure == null) {
                failure = new DataException("Redis rejected " + rejected + " commands of a batch that is "
                        + "otherwise applied and will not be delivered again, see the log for the records");
            }

            return new BatchResult(prepareMailRequests(pendingWrites), failure);
        } catch (RetriableException e) {
            // The refused transaction above; the connection is still good.
            throw e;
        } catch (ConnectException e) {
            // Raised above on purpose (offsets not stored, invalid stored offset):
            // keep its type and message, they say whether the batch was applied.
            logger.error("Cannot continue after this batch", e);
            closeRedisConnection();
            throw e;
        } catch (JedisAccessControlException e) {
            // Auth/ACL errors are permanent — retrying cannot fix them, so fail the
            // task loudly rather than hiding the misconfiguration behind retries.
            // (Subclass of JedisDataException, so it must be caught before it.)
            logger.error("Redis access control error", e);
            closeRedisConnection();
            throw new ConnectException("Redis access control error", e);
        } catch (JedisDataException e) {
            // Server refused a read or the whole transaction: the offset hash holds
            // another type, or Redis does not take writes right now (LOADING,
            // READONLY, MISCONF, OOM). Nothing was applied. Not retried here: the
            // task fails, and whatever restarts failed tasks retries with its own
            // backoff and gives up visibly when Redis stays that way.
            logger.error("Redis command/data error", e);
            closeRedisConnection();
            throw new DataException("Redis command/data error", e);
        } catch (JedisException e) {
            // Everything else under JedisException is infrastructure: connection
            // loss, pool exhaustion ("Could not get a resource from the pool") and
            // sentinel failover — all transient. Drop the connection and retry.
            logger.error("Redis connection error, will retry batch", e);
            closeRedisConnection();
            throw retry("Redis connection error", e, retryBackoffMs);
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

    /**
     * Returns the exception that makes Connect deliver the batch again after
     * {@code backoffMs}. Connect itself never stops doing so, and the task would stay
     * RUNNING while nothing is counted; after maxRetries in a row the task fails
     * instead, which shows, and hands over to whatever restarts failed tasks.
     */
    private RetriableException retry(String message, Throwable cause, long backoffMs) {
        if (retries >= maxRetries) {
            throw new ConnectException(message + ", giving up after " + retries + " retries", cause);
        }
        retries++;
        context.timeout(backoffMs);
        return new RetriableException(message, cause);
    }

    /**
     * Parses the records into {@code parsed} and returns the failure of the first one
     * that cannot be parsed, or null. Of a partition with such a record, only the
     * records before it are parsed.
     */
    private DataException parse(List<PendingWrite> pendingWrites, List<PendingWrite> parsed) {
        DataException failure = null;
        Set<TopicPartition> failedPartitions = new HashSet<TopicPartition>();

        for (PendingWrite write : pendingWrites) {
            if (failedPartitions.contains(write.partition)) {
                continue;
            }
            try {
                write.parse();
                parsed.add(write);
            } catch (JSONException e) {
                // Only JSON/field-shape problems are "bad data" — surface as a
                // non-retriable DataException. Anything unexpected propagates with
                // its real type rather than being mislabelled a parsing error.
                // The stored offset stays before this record, so the task fails on
                // it again until the offset is moved to it by hand (see the README).
                logger.error("Data or parsing error for record {} offset {}: {}", write.partition, write.offset,
                        write.record, e);
                failedPartitions.add(write.partition);
                if (failure == null) {
                    failure = new DataException("Data or parsing error for record " + write.partition
                            + " offset " + write.offset, e);
                }
            }
        }
        return failure;
    }

    /**
     * Fails the task when a partition does not continue where the task moved it to.
     * Kafka then did not have that offset and started the consumer over: the offset
     * stored in Redis is beyond the end of the partition, so it belongs to another
     * topic or another cluster. Carrying on would skip every record up to that offset
     * as already applied.
     */
    private void checkSoughtOffsets(Map<TopicPartition, Long> firstInHand) {
        for (Map.Entry<TopicPartition, Long> first : firstInHand.entrySet()) {
            TopicPartition partition = first.getKey();
            // Still the batch read before the seek was carried out.
            if (seekingPartitions.contains(partition)) {
                continue;
            }
            Long sought = soughtOffsets.remove(partition);
            if (sought != null && first.getValue() < sought) {
                throw new ConnectException("Asked Kafka to continue " + partition + " at offset " + sought
                        + ", the record after the last one applied to Redis, but it delivered offset "
                        + first.getValue() + ". The offset stored in Redis is beyond the end of the partition, "
                        + "so it belongs to another topic or cluster. Delete field " + partition.partition()
                        + " of the Redis hash " + offsetsKey(partition.topic()) + " to start from the "
                        + "consumer group's position.");
            }
        }
    }

    private void logRejected(String command, PendingWrite write, JedisDataException e) {
        logger.error("Redis rejected {} for key {} (record {} offset {}), the rest of the batch is applied: {}",
                command, write.key, write.partition, write.offset, e.getMessage());
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
     * Returns the offsets stored in Redis, per partition the offset of the last record
     * applied, for every topic of the batch and of the partitions still to be moved.
     * With {@code watch}, the offset hashes are WATCHed before they are read, so a
     * transaction that follows is refused when they changed in between.
     */
    private Map<TopicPartition, Long> readStoredOffsets(List<PendingWrite> pendingWrites, boolean watch) {
        Set<String> topics = new HashSet<String>();
        for (PendingWrite write : pendingWrites) {
            topics.add(write.partition.topic());
        }
        for (TopicPartition partition : unseekedPartitions) {
            topics.add(partition.topic());
        }

        if (watch) {
            List<String> keys = new ArrayList<String>();
            for (String topic : topics) {
                keys.add(offsetsKey(topic));
            }
            jedis.watch(keys.toArray(new String[0]));
        }

        Map<TopicPartition, Long> storedOffsets = new HashMap<TopicPartition, Long>();
        for (String topic : topics) {
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
     * to is replayed as well. A partition whose batch in hand already continues right
     * after the stored offset is left where it is.
     *
     * <p>A partition without a stored offset (first start with this version, or an
     * empty Redis) stays where the consumer group has it.
     */
    private void seekToStoredOffsets(Map<TopicPartition, Long> firstInHand, Map<TopicPartition, Long> lastInHand,
            Map<TopicPartition, Long> storedOffsets) {
        if (unseekedPartitions.isEmpty()) {
            return;
        }

        // The offsets were read before this is called: a failure to read them leaves
        // the partitions to the next put().
        Set<String> topicsWithOffsets = new HashSet<String>();
        for (TopicPartition stored : storedOffsets.keySet()) {
            topicsWithOffsets.add(stored.topic());
        }

        for (TopicPartition partition : unseekedPartitions) {
            Long storedOffset = storedOffsets.get(partition);
            if (storedOffset != null) {
                Long first = firstInHand.get(partition);
                if (first != null && first <= storedOffset + 1 && lastInHand.get(partition) > storedOffset
                        && !seekingPartitions.contains(partition)) {
                    // The batch in hand starts at or before the record after the
                    // stored offset and reaches beyond it, so it holds the records to
                    // continue with. Nothing to move or read again; what it holds at
                    // or below the stored offset is skipped as already applied.
                    //
                    // Not for a partition an earlier attempt at this batch requested
                    // a seek for: Connect carries that seek out, so the consumer may
                    // be elsewhere by now and has to be moved again.
                    logger.debug("{} continues in the batch in hand, after stored offset {}", partition,
                            storedOffset);
                    continue;
                }
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
                soughtOffsets.put(partition, storedOffset + 1);
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

    /** What a batch came to: the notifications to send, and what to fail the task with after that. */
    private static final class BatchResult {
        final List<HttpPost> mailRequests;
        final DataException failure;

        BatchResult(List<HttpPost> mailRequests, DataException failure) {
            this.mailRequests = mailRequests;
            this.failure = failure;
        }
    }

    /**
     * A sink record awaiting its write: first only where it was read from, then,
     * unless it is applied already, its parsed value. The responses hold the results
     * of its commands once the transaction is executed.
     */
    private static final class PendingWrite {
        final TopicPartition partition;
        final long offset;
        final SinkRecord record;
        String key;
        double billedCredits;
        long expireAt;
        JSONObject json;
        Response<Double> response;
        Response<Long> expireResponse;
        double newBilledCredits;
        boolean incrementRejected;

        PendingWrite(TopicPartition partition, long offset, SinkRecord record) {
            this.partition = partition;
            this.offset = offset;
            this.record = record;
        }

        void parse() throws JSONException {
            key = record.key() == null ? "" : record.key().toString();
            json = new JSONObject(record.value() == null ? "" : record.value().toString());
            billedCredits = json.getDouble("billed_credits");
            expireAt = json.getLong("expiration");
        }
    }
}
