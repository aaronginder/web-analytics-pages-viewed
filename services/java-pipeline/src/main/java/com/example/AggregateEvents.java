package com.example;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.json.JSONObject;


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
                .apply("CreateSession", Window.<KV<String, String>>into(Sessions.withGapDuration(Duration.standardSeconds(60)))
                    .triggering(AfterWatermark.pastEndOfWindow()
                        .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(30)))
                    )
                    .discardingFiredPanes()
                    .withAllowedLateness(Duration.standardSeconds(0))
                )
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
                JSONObject source = parsedEvents.get(i);               // keep original intact
                JSONObject event  = new JSONObject(source.toString()); // work on a copy

                long eventTsMs = source.getLong("timestamp_ms");
                event.put("timestamp_iso", Instant.ofEpochMilli(eventTsMs).toString());
                event.put("processing_timestamp_iso", Instant.now().toString());

                double timeSpentSec;
                if (i < parsedEvents.size() - 1) {
                    long nextTsMs = parsedEvents.get(i + 1).getLong("timestamp_ms");
                    timeSpentSec = (nextTsMs - eventTsMs) / 1000.0; // gap-based dwell
                } else {
                    JSONObject eventParams = event.optJSONObject("event_params");
                    long engaged = (eventParams != null) ? eventParams.optLong("engaged_time", 0L) : 0L;
                    timeSpentSec = engaged; // last event fallback
                }

                event.put("time_spent_on_page_seconds", timeSpentSec);
                event.put("sequence_number", i + 1);
                event.put("session_id", sessionId);

                if (i > 0) {
                    long prevTsMs = parsedEvents.get(i - 1).getLong("timestamp_ms");
                    event.put("previous_timestamp_iso", Instant.ofEpochMilli(prevTsMs).toString());
                }

                // Drop millis fields from output
                event.remove("timestamp_ms");
                event.remove("previous_timestamp_ms");

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
