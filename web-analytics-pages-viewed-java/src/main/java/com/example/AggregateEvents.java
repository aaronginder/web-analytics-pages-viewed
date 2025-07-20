package com.example;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.joda.time.Duration;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.json.JSONObject;
import org.apache.beam.sdk.values.KV;

public class AggregateEvents extends PTransform<PCollection<KV<String, String>>, PCollection<Iterable<String>>> {
    /**
     * Applies the transformation to aggregate events by session ID.
     *
     * @param input The input PCollection of key-value pairs where the key is the session ID
     *              and the value is an iterable of event JSON strings.
     * @return A PCollection of aggregated event JSON objects.
     */
    /**
     * DoFn that calculates time spent on page for a group of events.
     * This is currently not used in the pipeline but provides an example implementation.
     */

    @Override
    public PCollection<Iterable<String>> expand(PCollection<KV<String, String>> input) {
        return input
                .apply("CreateSession", Window.into(Sessions.withGapDuration(Duration.standardSeconds(5))))
                .apply("GroupBySessionId", org.apache.beam.sdk.transforms.GroupByKey.create())
                .apply("AddTimeSpentOnPage", ParDo.of(new AddTimeSpentOnPageFn()));
    }
    
    static class AddTimeSpentOnPageFn extends DoFn<KV<String, Iterable<String>>, Iterable<String>> {
        @ProcessElement
        public void processElement(@Element KV<String, Iterable<String>> element, OutputReceiver<Iterable<String>> receiver) {
            String sessionId = UUID.randomUUID().toString();
            List<JSONObject> parsedEvents = new ArrayList<>();
            
            // Parse all events in the group
            for (String event : element.getValue()) {
                parsedEvents.add(new JSONObject(event));
            }
            
            // Sort by timestamp
            parsedEvents.sort(Comparator.comparingLong(e -> e.getLong("timestamp_ms")));
            
            List<JSONObject> results = new ArrayList<>();
            for (int i = 0; i < parsedEvents.size(); i++) {
                JSONObject event = parsedEvents.get(i);
                
                long timeSpent;
                if (i < parsedEvents.size() - 1) {
                    timeSpent = (parsedEvents.get(i + 1).getLong("timestamp_ms") - event.getLong("timestamp_ms")) / 1000;
                } else {
                    // Last event in session
                    JSONObject eventParams = event.getJSONObject("event_params");
                    timeSpent = eventParams.has("engaged_time") ? eventParams.getLong("engaged_time") : 0;
                }
                
                event.put("time_spent_on_page_seconds", timeSpent);
                event.put("sequence_number", i + 1);
                event.put("session_id", sessionId);
                
                if (i > 0) {
                    event.put("previous_time", parsedEvents.get(i - 1).getString("timestamp_dtm"));
                }
                
                results.add(event);
            }
            
            // Convert JSONObjects to strings correctly
            List<String> jsonStrings = new ArrayList<>();
            for (JSONObject event : results) {
                jsonStrings.add(event.toString());
            }
            
            // Output the list of strings (which is an Iterable<String>)
            receiver.output(jsonStrings);
        }
    }
}
