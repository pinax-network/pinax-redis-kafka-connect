package com.pinax.kafka.redis.connect.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

class MailContentTest {

    @Test
    void toJSONArray_buildsMandrillMergeVars() {
        MailContent content = new MailContent("Acme", "Pro", "123.45", "100.00");

        JSONArray vars = content.toJSONArray();

        assertEquals(4, vars.length());
        assertMergeVar(vars.getJSONObject(0), "TeamName", "Acme");
        assertMergeVar(vars.getJSONObject(1), "TeamPlan", "Pro");
        assertMergeVar(vars.getJSONObject(2), "BilledCredits", "123.45");
        assertMergeVar(vars.getJSONObject(3), "IncludedCredits", "100.00");
    }

    @Test
    void gettersReturnConstructorValues() {
        MailContent content = new MailContent("Acme", "Pro", "123.45", "100.00");

        assertEquals("Acme", content.getTeamName());
        assertEquals("Pro", content.getTeamPlan());
        assertEquals("123.45", content.getBilledCredits());
        assertEquals("100.00", content.getIncludedCredits());
    }

    private static void assertMergeVar(JSONObject obj, String name, String contentValue) {
        assertEquals(name, obj.getString("name"));
        assertEquals(contentValue, obj.getString("content"));
    }
}
