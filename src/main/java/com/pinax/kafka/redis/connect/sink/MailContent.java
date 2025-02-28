package com.pinax.kafka.redis.connect.sink;

import org.json.JSONArray;
import org.json.JSONObject;

public class MailContent {
    private String teamName;
    private String teamPlan;
    private Double billedCredits;
    private Integer includedCredits;

    public MailContent(String teamName, String teamPlan, Double billedCredits, Integer includedCredits) {
        this.teamName = teamName;
        this.teamPlan = teamPlan;
        this.billedCredits = billedCredits;
        this.includedCredits = includedCredits;
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

    public JSONArray toJSONArray() {
        return new JSONArray()
                .put(new JSONObject().put("name", "TeamName").put("content", this.teamName))
                .put(new JSONObject().put("name", "TeamPlan").put("content", this.teamPlan))
                .put(new JSONObject().put("name", "BilledCredits").put("content", this.billedCredits))
                .put(new JSONObject().put("name", "IncludedCredits").put("content", this.includedCredits));
    }
}
