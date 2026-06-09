package com.pinax.kafka.redis.connect.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.json.JSONException;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link MailSender#findMailFailures(String)} — the logic that
 * decides whether a Mandrill 200 response actually delivered the mail. Mandrill
 * answers HTTP 200 even when it rejects a recipient, so this guards against
 * silently logging a rejected send as a success.
 */
class MailSenderTest {

    @Test
    void allAcceptedStatuses_returnNoFailures() {
        String body = "[{\"email\":\"a@x.test\",\"status\":\"sent\"},"
                + "{\"email\":\"b@x.test\",\"status\":\"queued\"},"
                + "{\"email\":\"c@x.test\",\"status\":\"scheduled\"}]";

        assertTrue(MailSender.findMailFailures(body).isEmpty());
    }

    @Test
    void rejectedRecipient_isReportedAsFailure() {
        String body = "[{\"email\":\"a@x.test\",\"status\":\"rejected\",\"reject_reason\":\"hard-bounce\"}]";

        List<String> failures = MailSender.findMailFailures(body);

        assertEquals(1, failures.size());
        assertTrue(failures.get(0).contains("a@x.test"));
        assertTrue(failures.get(0).contains("rejected"));
        assertTrue(failures.get(0).contains("hard-bounce"));
    }

    @Test
    void invalidRecipient_isReportedAsFailure() {
        String body = "[{\"email\":\"bad@x.test\",\"status\":\"invalid\"}]";

        List<String> failures = MailSender.findMailFailures(body);

        assertEquals(1, failures.size());
        assertTrue(failures.get(0).contains("invalid"));
    }

    @Test
    void mixedRecipients_reportOnlyTheNonDelivered() {
        String body = "[{\"email\":\"ok@x.test\",\"status\":\"sent\"},"
                + "{\"email\":\"bad@x.test\",\"status\":\"rejected\",\"reject_reason\":\"spam\"}]";

        List<String> failures = MailSender.findMailFailures(body);

        assertEquals(1, failures.size());
        assertTrue(failures.get(0).contains("bad@x.test"));
    }

    @Test
    void apiErrorObject_isReportedAsFailure() {
        String body = "{\"status\":\"error\",\"code\":-1,\"name\":\"Invalid_Key\","
                + "\"message\":\"Invalid API key\"}";

        List<String> failures = MailSender.findMailFailures(body);

        assertEquals(1, failures.size());
        assertTrue(failures.get(0).contains("Invalid_Key"));
    }

    @Test
    void unexpectedJsonObject_isReportedAsFailureNotSuccess() {
        // A successful send-template response is always a JSON array. A bare object
        // that is not an explicit error must NOT be treated as a silent success.
        List<String> failures = MailSender.findMailFailures("{\"foo\":\"bar\"}");

        assertEquals(1, failures.size());
        assertTrue(failures.get(0).contains("Unexpected"));
    }

    @Test
    void malformedBody_throwsSoCallerTreatsItAsFailure() {
        assertThrows(JSONException.class, () -> MailSender.findMailFailures("not json"));
    }
}
