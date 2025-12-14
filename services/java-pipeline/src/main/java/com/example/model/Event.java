package com.example.model;

import java.io.Serializable;
import java.util.Objects;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.json.JSONObject;

@DefaultCoder(SerializableCoder.class)
public class Event implements Serializable {
    private String eventName;
    private String userId;
    private String pageId;
    private Long timestampMs;
    private EventParams eventParams;

    public Event() {}

    public Event(String eventName, String userId, String pageId, Long timestampMs, EventParams eventParams) {
        this.eventName = eventName;
        this.userId = userId;
        this.pageId = pageId;
        this.timestampMs = timestampMs;
        this.eventParams = eventParams;
    }

    public static Event fromJson(String raw) {
        JSONObject obj = new JSONObject(raw);
        String eventName = obj.optString("event_name", null);
        String userId = obj.optString("user_id", null);
        String pageId = obj.optString("page_id", null);
        Long timestampMs = obj.has("timestamp_ms") && !obj.isNull("timestamp_ms") ? obj.getLong("timestamp_ms") : null;
        EventParams params = EventParams.fromJson(obj.optJSONObject("event_params"));
        return new Event(eventName, userId, pageId, timestampMs, params);
    }

    public String getEventName() { return eventName; }
    public String getUserId() { return userId; }
    public String getPageId() { return pageId; }
    public Long getTimestampMs() { return timestampMs; }
    public EventParams getEventParams() { return eventParams; }

    public void setEventName(String eventName) { this.eventName = eventName; }
    public void setUserId(String userId) { this.userId = userId; }
    public void setPageId(String pageId) { this.pageId = pageId; }
    public void setTimestampMs(Long timestampMs) { this.timestampMs = timestampMs; }
    public void setEventParams(EventParams eventParams) { this.eventParams = eventParams; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Event)) return false;
        Event event = (Event) o;
        return Objects.equals(eventName, event.eventName)
                && Objects.equals(userId, event.userId)
                && Objects.equals(pageId, event.pageId)
                && Objects.equals(timestampMs, event.timestampMs)
                && Objects.equals(eventParams, event.eventParams);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventName, userId, pageId, timestampMs, eventParams);
    }
}
