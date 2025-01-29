package com.pinax.kafka.redis.connect.sink;

import org.simplejavamail.MailException;
import org.simplejavamail.api.email.Email;
import org.simplejavamail.api.mailer.Mailer;
import org.simplejavamail.email.EmailBuilder;
import org.simplejavamail.mailer.MailerBuilder;

public class MailSender {

    private Mailer mailer = null;
    private Email email = null;

    MailSender(String from, String to, String host, int port, String username, String password) {
        initMailer(host, port, username, password);
        initEmail(from, to);
    }

    private void initMailer(String host, int port, String username, String password) {
        // Initialize mailer
        mailer = MailerBuilder
                .withSMTPServer(host, port, username, password)
                .buildMailer();

    }

    private void initEmail(String from, String to) {
        // Initialize email
        email = EmailBuilder.startingBlank()
                .from(from)
                .to(to)
                .buildEmail();
    }

    public void SendUsageMail() {
        // Send mail
        boolean isEmailValid = mailer.validate(email);

        if (isEmailValid) {
            try {
                // TODO: send email
            } catch (MailException e) {
                // TODO: throw exception
            }
        } else {
            // TODO: throw exception
        }
    }
}
