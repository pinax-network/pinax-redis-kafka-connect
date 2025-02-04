package com.pinax.kafka.redis.connect.sink;

import org.json.JSONArray;
import org.json.JSONObject;

public class MailContent {
    private String fullname;
    private int usage;

    public MailContent(String fullname, int usage) {
        this.fullname = fullname;
        this.usage = usage;
    }

    public String getFullname() {
        return fullname;
    }

    public int getUsage() {
        return usage;
    }

    public void setFullname(String fullname) {
        this.fullname = fullname;
    }

    public void setUsage(int usage) {
        this.usage = usage;
    }

    public JSONArray toJSONArray() {
        return new JSONArray()
                .put(new JSONObject().put("name", "fullname").put("content", this.fullname))
                .put(new JSONObject().put("name", "usage").put("content", this.usage));
    }
}
