package com.example.model;

import java.io.Serializable;
import java.util.Objects;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.json.JSONObject;

@DefaultCoder(SerializableCoder.class)
public class EventParams implements Serializable {
    private Boolean testEvent;
    private Long engagedTime;
    private String pageTitle;
    private String trafficSource;

    public EventParams() {}

    public EventParams(Boolean testEvent, Long engagedTime, String pageTitle, String trafficSource) {
        this.testEvent = testEvent;
        this.engagedTime = engagedTime;
        this.pageTitle = pageTitle;
        this.trafficSource = trafficSource;
    }

    public static EventParams fromJson(JSONObject obj) {
        if (obj == null) {
            return null;
        }
        Boolean testEventVal = obj.has("test_event") && !obj.isNull("test_event") ? obj.optBoolean("test_event") : null;
        Long engagedTimeVal = obj.has("engaged_time") && !obj.isNull("engaged_time") ? obj.optLong("engaged_time") : null;
        String pageTitleVal = obj.optString("page_title", null);
        String trafficSourceVal = obj.optString("traffic_source", null);
        return new EventParams(testEventVal, engagedTimeVal, pageTitleVal, trafficSourceVal);
    }

    public Boolean getTestEvent() { return testEvent; }
    public Long getEngagedTime() { return engagedTime; }
    public String getPageTitle() { return pageTitle; }
    public String getTrafficSource() { return trafficSource; }

    public void setTestEvent(Boolean testEvent) { this.testEvent = testEvent; }
    public void setEngagedTime(Long engagedTime) { this.engagedTime = engagedTime; }
    public void setPageTitle(String pageTitle) { this.pageTitle = pageTitle; }
    public void setTrafficSource(String trafficSource) { this.trafficSource = trafficSource; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EventParams)) return false;
        EventParams that = (EventParams) o;
        return Objects.equals(testEvent, that.testEvent)
                && Objects.equals(engagedTime, that.engagedTime)
                && Objects.equals(pageTitle, that.pageTitle)
                && Objects.equals(trafficSource, that.trafficSource);
    }

    @Override
    public int hashCode() {
        return Objects.hash(testEvent, engagedTime, pageTitle, trafficSource);
    }
}
