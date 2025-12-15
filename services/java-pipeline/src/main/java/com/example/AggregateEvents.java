package com.example;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.json.JSONObject;
import com.example.model.Event;
import com.example.model.EventParams;


public class AggregateEvents extends PTransform<PCollection<KV<String, Event>>, PCollection<Iterable<String>>> {
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
    public PCollection<Iterable<String>> expand(PCollection<KV<String, Event>> input) {
        return input
                .apply("CreateSession", Window.<KV<String, Event>>into(Sessions.withGapDuration(Duration.standardSeconds(60)))
                        .triggering(AfterWatermark.pastEndOfWindow()
                                .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(30)))
                        )
                        .accumulatingFiredPanes()
                        .withAllowedLateness(Duration.standardSeconds(0))
                )
                .apply("GroupBySessionId", org.apache.beam.sdk.transforms.GroupByKey.create())
                .apply("AddTimeSpentOnPage", ParDo.of(new AddTimeSpentOnPageFn()));
    }
    
    static class AddTimeSpentOnPageFn extends DoFn<KV<String, Iterable<Event>>, Iterable<String>> {
        @ProcessElement
        public void processElement(@Element KV<String, Iterable<Event>> element, OutputReceiver<Iterable<String>> receiver) {
            String sessionId = UUID.randomUUID().toString();
            List<Event> events = new ArrayList<>();

            for (Event event : element.getValue()) {
                if (event != null && event.getTimestampMs() != null) {
                    events.add(event);
                }
            }
            if (events.isEmpty()) {
                return;
            }

            // Sort by timestamp
            events.sort(Comparator.comparingLong(Event::getTimestampMs));

            List<JSONObject> results = new ArrayList<>();
            for (int i = 0; i < events.size(); i++) {
                Event ev = events.get(i);

                long eventTsMs = ev.getTimestampMs();
                JSONObject out = new JSONObject();
                out.put("event_name", ev.getEventName());
                out.put("user_id", ev.getUserId());
                out.put("page_id", ev.getPageId());
                out.put("timestamp_iso", Instant.ofEpochMilli(eventTsMs).toString());
                out.put("processing_timestamp_iso", Instant.now().toString());

                double timeSpentSec;
                if (i < events.size() - 1) {
                    long nextTsMs = events.get(i + 1).getTimestampMs();
                    timeSpentSec = (nextTsMs - eventTsMs) / 1000.0;
                } else {
                    EventParams params = ev.getEventParams();
                    long engaged = (params != null && params.getEngagedTime() != null) ? params.getEngagedTime() : 0L;
                    timeSpentSec = engaged;
                }

                out.put("time_spent_on_page_seconds", timeSpentSec);
                out.put("sequence_number", i + 1);
                out.put("session_id", sessionId);

                if (i > 0) {
                    long prevTsMs = events.get(i - 1).getTimestampMs();
                    out.put("previous_timestamp_iso", Instant.ofEpochMilli(prevTsMs).toString());
                }

                // event_params passthrough
                EventParams params = ev.getEventParams();
                JSONObject paramsOut = new JSONObject();
                if (params != null) {
                    if (params.getEngagedTime() != null) paramsOut.put("engaged_time", params.getEngagedTime());
                    if (params.getPageTitle() != null) paramsOut.put("page_title", params.getPageTitle());
                    if (params.getTrafficSource() != null) paramsOut.put("traffic_source", params.getTrafficSource());
                    if (params.getTestEvent() != null) paramsOut.put("test_event", params.getTestEvent());
                }
                out.put("event_params", paramsOut);

                results.add(out);
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
