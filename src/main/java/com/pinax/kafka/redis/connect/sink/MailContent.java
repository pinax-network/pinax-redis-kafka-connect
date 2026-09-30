package com.pinax.kafka.redis.connect.sink;

import org.json.JSONArray;
import org.json.JSONObject;

public class MailContent {
    private String teamName;
    private String teamPlan;
    private String billedCredits;
    private String includedCredits;
    // The usage milestone crossed, as a whole percentage of the included credits (50, 75, 90, 100, 150 or 200).
    private String usagePercent;

    public MailContent(String teamName, String teamPlan, String billedCredits, String includedCredits, String usagePercent) {
        this.teamName = teamName;
        this.teamPlan = teamPlan;
        this.billedCredits = billedCredits;
        this.includedCredits = includedCredits;
        this.usagePercent = usagePercent;
    }

    public String getTeamName() {
        return teamName;
    }

    public String getTeamPlan() {
        return teamPlan;
    }

    public String getBilledCredits() {
        return billedCredits;
    }

    public String getIncludedCredits() {
        return includedCredits;
    }

    public String getUsagePercent() {
        return usagePercent;
    }

    public JSONArray toJSONArray() {
        return new JSONArray()
                .put(new JSONObject().put("name", "TeamName").put("content", this.teamName))
                .put(new JSONObject().put("name", "TeamPlan").put("content", this.teamPlan))
                .put(new JSONObject().put("name", "BilledCredits").put("content", this.billedCredits))
                .put(new JSONObject().put("name", "IncludedCredits").put("content", this.includedCredits))
                .put(new JSONObject().put("name", "UsagePercent").put("content", this.usagePercent));
    }
}
