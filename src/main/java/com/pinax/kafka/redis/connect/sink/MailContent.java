package com.pinax.kafka.redis.connect.sink;

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
}
