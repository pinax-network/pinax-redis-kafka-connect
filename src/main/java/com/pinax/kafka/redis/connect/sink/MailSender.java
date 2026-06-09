package com.pinax.kafka.redis.connect.sink;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MailSender implements Closeable {
    private final Logger logger = LoggerFactory.getLogger(RedisSinkConnector.class);

    private static final String MAILCHIMP_API_URL = "https://mandrillapp.com/api/1.0/messages/send-template";

    // Mandrill per-recipient statuses that mean the message was accepted for
    // delivery. Any other status ("rejected", "invalid") means it was NOT sent.
    private static final Set<String> ACCEPTED_STATUSES = Set.of("sent", "queued", "scheduled");

    private final String from;
    private final String mailChimpApiKey;
    private final String templateSlug;

    // Shared, thread-safe client backed by a pooling connection manager. One per
    // MailSender for its whole lifetime — sends run concurrently from put() and
    // reuse pooled connections instead of leaking a client+sockets per email.
    private final CloseableHttpClient httpClient;

    MailSender(String from, String mailChimpApiKey, String templateSlug) {
        this.from = from;
        this.mailChimpApiKey = mailChimpApiKey;
        this.templateSlug = templateSlug;
        this.httpClient = HttpClientBuilder.create().build();
    }

    private JSONObject prepareBody(String to, String subject, MailContent mailContent) {
        JSONArray mailTemplateContent = mailContent.toJSONArray();

        JSONObject message = new JSONObject();
        message.put("to", new JSONArray().put(new JSONObject().put("email", to).put("type", "to")));
        message.put("from_email", this.from);
        message.put("subject", subject);
        message.put("global_merge_vars", mailTemplateContent);

        JSONObject json = new JSONObject();
        json.put("key", this.mailChimpApiKey);
        json.put("template_name", this.templateSlug);
        json.put("template_content", mailTemplateContent);
        json.put("message", message);

        return json;
    }

    public HttpPost CreateUsageMailRequest(String to, String subject, MailContent mailContent) {
        HttpPost httpPost = new HttpPost(MAILCHIMP_API_URL);

        JSONObject body = prepareBody(to, subject, mailContent);
        final StringEntity stringEntity = new StringEntity(body.toString());
        httpPost.setEntity(stringEntity);
        httpPost.setHeader("Accept", "application/json");
        httpPost.setHeader("Content-type", "application/json");

        return httpPost;
    }

    public void SendUsageMailRequest(HttpPost usageMailRequest) {
        try {
            httpClient.execute(usageMailRequest, response -> {
                int status = response.getCode();
                HttpEntity entity = response.getEntity();
                String responseString = entity == null ? "" : EntityUtils.toString(entity);

                if (status < 200 || status >= 300) {
                    // Transport / API-level failure (e.g. an invalid key returns HTTP 5xx).
                    // Throwing routes it to the catch below, where it is logged as an error.
                    throw new IOException("Failed to send usage mail (HTTP " + status + "): " + responseString);
                }

                // Mandrill answers HTTP 200 even when it rejects a recipient, so a 2xx is
                // NOT proof of delivery. Inspect the body and log any rejection as an error
                // instead of reporting a silent "success".
                try {
                    List<String> failures = findMailFailures(responseString);
                    if (failures.isEmpty()) {
                        logger.info("Usage mail sent successfully");
                    } else {
                        logger.error("Usage mail not delivered: {}", failures);
                    }
                } catch (JSONException e) {
                    // An unparseable / unexpected body is a failure signal, not a success.
                    logger.error("Unexpected Mandrill response, treating as failure: {}", responseString, e);
                }

                return responseString;
            });
        } catch (IOException e) {
            logger.error("Failed to send usage mail", e);
        }
    }

    /**
     * Inspects a Mandrill {@code messages/send-template} response body. A successful
     * call returns a JSON array of per-recipient results, each with a {@code status}
     * of {@code sent}/{@code queued}/{@code scheduled} (accepted) or
     * {@code rejected}/{@code invalid} (not sent); an API-level error is a JSON object
     * with {@code "status":"error"}. Returns a human-readable description for every
     * non-delivered recipient — empty <em>only</em> when the body is an array and every
     * recipient was accepted. Any non-array body (an error object or an otherwise
     * unexpected shape) yields a failure entry, so an unexpected response is never
     * mistaken for a success.
     *
     * <p>Visible for testing. Throws {@link JSONException} if the body cannot be parsed
     * as JSON at all, which the caller treats as a delivery failure.
     */
    static List<String> findMailFailures(String responseBody) {
        List<String> failures = new ArrayList<>();
        Object parsed = new JSONTokener(responseBody).nextValue();

        if (parsed instanceof JSONArray) {
            JSONArray results = (JSONArray) parsed;
            for (int i = 0; i < results.length(); i++) {
                JSONObject result = results.getJSONObject(i);
                String recipientStatus = result.optString("status", "");
                if (!ACCEPTED_STATUSES.contains(recipientStatus)) {
                    failures.add(result.optString("email", "?") + ": status=" + recipientStatus
                            + ", reject_reason=" + result.optString("reject_reason", ""));
                }
            }
        } else if (parsed instanceof JSONObject) {
            // A successful send-template response is always a JSON array, so any object
            // here is either an explicit API error or an unexpected shape — never a
            // delivery. Surface both rather than reporting a silent success.
            JSONObject result = (JSONObject) parsed;
            if ("error".equals(result.optString("status", ""))) {
                failures.add("API error " + result.optString("name", "")
                        + ": " + result.optString("message", ""));
            } else {
                failures.add("Unexpected Mandrill response: " + result);
            }
        } else {
            throw new JSONException("Unexpected Mandrill response shape");
        }

        return failures;
    }

    @Override
    public void close() {
        try {
            httpClient.close();
        } catch (IOException e) {
            logger.warn("Error while closing mail HTTP client", e);
        }
    }
}
