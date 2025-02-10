package com.pinax.kafka.redis.connect.sink;

import org.json.JSONArray;
import org.json.JSONObject;

public class MailContent {
    private String teamName;
    private String teamPlan;
    private Double billedCredits;
    private Integer includedCredits;
    private Integer creditCutoff;

    public MailContent(String teamName, String teamPlan, Double billedCredits, Integer includedCredits, Integer creditCutoff) {
        this.teamName = teamName;
        this.teamPlan = teamPlan;
        this.billedCredits = billedCredits;
        this.includedCredits = includedCredits;
        this.creditCutoff = creditCutoff;
    }

    public String getTeamName() {
        return teamName;
    }

    public String getTeamPlan() {
        return teamPlan;
    }

    public Double getBilledCredits() {
        return billedCredits;
    }

    public Integer getIncludedCredits() {
        return includedCredits;
    }

    public Integer getCreditCutoff() {
        return creditCutoff;
    }

    public JSONArray toJSONArray() {
        return new JSONArray()
                .put(new JSONObject().put("name", "teamname").put("content", this.teamName))
                .put(new JSONObject().put("name", "teamplan").put("content", this.teamPlan))
                .put(new JSONObject().put("name", "billedcredits").put("content", this.billedCredits))
                .put(new JSONObject().put("name", "includedcredits").put("content", this.includedCredits))
                .put(new JSONObject().put("name", "creditcutoff").put("content", this.creditCutoff));
    }
}
