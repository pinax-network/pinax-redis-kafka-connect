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
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.json.JSONObject;
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

        List<JSONObject> jsonObjects = new ArrayList<JSONObject>();
        List<Response<Double>> newBilledCreditsResponses = new ArrayList<Response<Double>>();

        for (SinkRecord record : records) {
            try {
                logger.debug("Processing record: {}", record);
                key = record.key() == null ? "" : record.key().toString();
                value = record.value() == null ? "" : record.value().toString();

                JSONObject json = new JSONObject(value);
                jsonObjects.add(json);

                double billedCredits = json.getDouble("billed_credits");
                long expireAtValue = json.getLong("expiration");

                Response<Double> newBilledCreditsResponse = jedisPipeline.incrByFloat(key, billedCredits);
                newBilledCreditsResponses.add(newBilledCreditsResponse);

                jedisPipeline.expireAt(key, expireAtValue);

                logger.debug("Record written to Redis: key={}, value={}", key, value);

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

        List<HttpPost> mailRequests = new ArrayList<HttpPost>();

        try {
            jedisPipeline.sync();

            mailRequests = prepareMailRequests(jsonObjects, newBilledCreditsResponses);
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

        List<CompletableFuture<Void>> futures = new ArrayList<CompletableFuture<Void>>();
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

    private List<HttpPost> prepareMailRequests(List<JSONObject> jsonObjects,
            List<Response<Double>> newBilledCreditsResponses) {
        List<HttpPost> mailRequests = new ArrayList<HttpPost>();

        // Prepare mail requests
        for (int i = 0; i < jsonObjects.size(); i++) {
            JSONObject json = jsonObjects.get(i);
            Response<Double> newBilledCreditsResponse = newBilledCreditsResponses.get(i);

            double billedCredits = json.getDouble("billed_credits");
            String teamBillingEmail = json.getString("team_billing_email");
            String teamName = json.getString("team_name");
            String teamPlan = json.getString("team_plan");
            Integer includedCredits = json.getInt("included_credits");
            Integer creditCutoff = json.getInt("credit_cutoff");

            double newBilledCredits = newBilledCreditsResponse.get();
            double oldBilledCredits = newBilledCreditsResponse.get() - billedCredits;

            // if (creditCutoff > 0) {

            // List<Double> creditThresholds = new ArrayList<Double>();
            // creditThresholds.add(creditCutoff * 0.50);
            // creditThresholds.add(creditCutoff * 0.75);
            // creditThresholds.add(creditCutoff * 0.90);
            // creditThresholds.add(creditCutoff * 1.00);

            // for (Double creditThreshold : creditThresholds) {
            // if (oldBilledCredits < creditThreshold && newBilledCredits >=
            // creditThreshold) {
            // MailContent mailContent = new MailContent(teamName, teamPlan,
            // newBilledCredits,
            // includedCredits);
            // mailRequests.add(mailSender.CreateUsageMailRequest(teamBillingEmail,
            // "An Update on your Monthly Usage", mailContent)); // TODO: Change the mail
            // subject
            // break;
            // }
            // }
            // }

            if (includedCredits > 0 && includedCredits != creditCutoff) {

                List<Double> creditThresholds = new ArrayList<Double>();
                creditThresholds.add(includedCredits * 0.50);
                creditThresholds.add(includedCredits * 0.75);
                creditThresholds.add(includedCredits * 0.90);
                creditThresholds.add(includedCredits * 1.00);
                creditThresholds.add(includedCredits * 1.50);
                creditThresholds.add(includedCredits * 2.00);

                // Format to currency and remove currency symbol
                DecimalFormat formatter = (DecimalFormat) NumberFormat.getCurrencyInstance(Locale.US);
                DecimalFormatSymbols symbols = formatter.getDecimalFormatSymbols();
                symbols.setCurrencySymbol("");
                formatter.setDecimalFormatSymbols(symbols);

                for (Double creditThreshold : creditThresholds) {
                    if (oldBilledCredits < creditThreshold && newBilledCredits >= creditThreshold) {
                        String newBilledCreditsString = formatter.format(newBilledCredits);
                        String includedCreditsString = formatter.format(includedCredits);

                        MailContent mailContent = new MailContent(teamName, teamPlan, newBilledCreditsString,
                                includedCreditsString);
                        mailRequests.add(mailSender.CreateUsageMailRequest(teamBillingEmail,
                                "An Update on your Monthly Usage", mailContent)); // TODO: Change the mail subject
                        break;
                    }
                }
            }
        }

        return mailRequests;
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
