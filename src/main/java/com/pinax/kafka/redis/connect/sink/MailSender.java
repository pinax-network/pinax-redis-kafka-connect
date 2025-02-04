package com.pinax.kafka.redis.connect.sink;

import java.io.IOException;

import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MailSender {
    private final Logger logger = LoggerFactory.getLogger(RedisSinkConnector.class);

    private final String from;
    private final String mailChimpApiKey;
    private final String templateSlug;

    private static final String MAILCHIMP_API_URL = "https://mandrillapp.com/api/1.0/messages/send-template";

    MailSender(String from, String mailChimpApiKey, String templateSlug) {
        this.from = from;
        this.mailChimpApiKey = mailChimpApiKey;
        this.templateSlug = templateSlug;
    }

    // Formats the mail content to fit with the template params
    private String formatTemplateContent(MailContent mailContent) {
        return "[{\"name\":\"fullname\", \"content\":\"" + mailContent.getFullname() + "\"}, {\"name\":\"usage\", \"content\":\"" + mailContent.getUsage() + "\"}]";
    }

    public void SendUsageMail(String to, String subject, MailContent mailContent) {
        String mailTemplateContent = formatTemplateContent(mailContent);

        // 2. Send mail and handle exceptions
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        HttpPost httpPost = new HttpPost(MAILCHIMP_API_URL);

        final String json = "{\"key\": \"" + this.mailChimpApiKey + "\", \"template_name\": \""+ this.templateSlug +"\", \"template_content\": " + mailTemplateContent + ", \"message\": {\"to\": [{\"email\":\""+ to +"\",\"type\":\"to\"}],\"from_email\":\""+ this.from +"\",\"subject\":\"" + subject + "\", \"global_merge_vars\": " + mailTemplateContent + "}}";
        final StringEntity stringEntity = new StringEntity(json);
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
