package com.pinax.kafka.redis.connect.sink;

import java.io.IOException;

import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MailSender {
    private final Logger logger = LoggerFactory.getLogger(RedisSinkConnector.class);
    
    private static final String MAILCHIMP_API_URL = "https://mandrillapp.com/api/1.0/messages/send-template";

    private final String from;
    private final String mailChimpApiKey;
    private final String templateSlug;


    MailSender(String from, String mailChimpApiKey, String templateSlug) {
        this.from = from;
        this.mailChimpApiKey = mailChimpApiKey;
        this.templateSlug = templateSlug;
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
        

    public void SendUsageMail(String to, String subject, MailContent mailContent) {
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        HttpPost httpPost = new HttpPost(MAILCHIMP_API_URL);

        JSONObject body = prepareBody(to, subject, mailContent);
        final StringEntity stringEntity = new StringEntity(body.toString());
        httpPost.setEntity(stringEntity);
        httpPost.setHeader("Accept", "application/json");
        httpPost.setHeader("Content-type", "application/json");

        try {
            httpClient.execute(httpPost, response -> {
                int status = response.getCode();
                if (status >= 200 && status < 300) {
                    HttpEntity entity = response.getEntity();
                    logger.info("Mail sent successfully to: " + to + ", {}", mailContent);
                    return entity;
                } else {
                    HttpEntity entity = response.getEntity();
                    String responseString = EntityUtils.toString(entity);
                    throw new IOException("Failed to send mail to: " + to + ", " + responseString);
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
