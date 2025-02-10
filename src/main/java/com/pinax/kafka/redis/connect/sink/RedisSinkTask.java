package com.pinax.kafka.redis.connect.sink;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import org.apache.hc.client5.http.classic.methods.HttpPost;
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

        // Prepare email sender
        mailSender = new MailSender(
                config.getString(RedisSinkConfig.FROM),
                config.getString(RedisSinkConfig.MAILCHIMP_API_KEY),
                config.getString(RedisSinkConfig.TEMPLATE_SLUG));

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

        List<HttpPost> mailRequests = new ArrayList<HttpPost>();

        for (SinkRecord record : records) {
            try {
                logger.debug("Processing record: {}", record);
                key = record.key() == null ? "" : record.key().toString();
                value = record.value() == null ? "" : record.value().toString();

                // billedCredits:expirationTimestamp:teamName:teamPlan:IncludedCredits:creditCutoff
                String[] parts = value.split(":");
                if (parts.length < 7) {
                    throw new DataException("Invalid value format: " + value);
                }

                double billedCredits = Double.parseDouble(parts[0]);
                long expireAtValue = Long.parseLong(parts[1]);
                String teamBillingEmail = parts[2];
                String teamName = parts[3];
                String teamPlan = parts[4];
                Integer includedCredits = Integer.parseInt(parts[5]);
                Integer creditCutoff = Integer.parseInt(parts[6]);

                Response<Double> newBilledCredits = jedisPipeline.incrByFloat(key, billedCredits);
                jedisPipeline.expireAt(key, expireAtValue);
                logger.debug("Record written to Redis: key={}, value={}", key, value);

                double oldBilledCredits = newBilledCredits.get() - billedCredits;

                // Create usage mail request if needed

                // send cut off threshold reached email if necessary
                if (creditCutoff > 0) {

                    List<Double> creditThresholds = new ArrayList<Double>();
                    creditThresholds.add(creditCutoff * 0.50);
                    creditThresholds.add(creditCutoff * 0.75);
                    creditThresholds.add(creditCutoff * 0.90);
                    creditThresholds.add(creditCutoff * 1.00);

                    for (Double creditThreshold : creditThresholds) {
                        if (oldBilledCredits < creditThreshold && newBilledCredits.get() >= creditThreshold) {
                            // send cut off threshold reached email
                            MailContent mailContent = new MailContent(teamName, teamPlan, newBilledCredits.get(),
                                    includedCredits, creditCutoff);
                            mailRequests.add(mailSender.CreateUsageMailRequest(teamBillingEmail,
                                    "Cut Off Credits Email", mailContent)); // TODO: Change the mail subject
                            break;
                        }
                    }
                }

                // send included credit threshold reached email if necessary
                if (includedCredits > 0 && includedCredits != creditCutoff) {

                    List<Double> creditThresholds = new ArrayList<Double>();
                    creditThresholds.add(includedCredits * 0.50);
                    creditThresholds.add(includedCredits * 0.75);
                    creditThresholds.add(includedCredits * 0.90);
                    creditThresholds.add(includedCredits * 1.00);
                    creditThresholds.add(includedCredits * 1.50);
                    creditThresholds.add(includedCredits * 2.00);

                    for (Double creditThreshold : creditThresholds) {
                        if (oldBilledCredits < creditThreshold && newBilledCredits.get() >= creditThreshold) {
                            // send included credit threshold reached email
                            MailContent mailContent = new MailContent(teamName, teamPlan, newBilledCredits.get(),
                                    includedCredits, creditCutoff);
                            mailRequests.add(mailSender.CreateUsageMailRequest(teamBillingEmail,
                                    "Included Credits Email", mailContent)); // TODO: Change the mail subject
                            break;
                        }
                    }
                }
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

        mailRequests.forEach(mailRequest -> {
            try {
                mailSender.SendUsageMailRequest(mailRequest); // TODO: Make this async
            } catch (Exception e) {
                logger.error("Failed to send mail", e);
            }
        });
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
