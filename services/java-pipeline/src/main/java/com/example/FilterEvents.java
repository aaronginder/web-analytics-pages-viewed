package com.example;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.example.model.Event;
import com.example.model.EventParams;

/** Filters out test events where event_params.test_event == true. */
public class FilterEvents extends PTransform<PCollection<KV<String, Event>>, PCollection<KV<String, Event>>> {

    @Override
    public PCollection<KV<String, Event>> expand(PCollection<KV<String, Event>> input) {
        return input.apply("FilterTestEvents", ParDo.of(new FilterTestEventsFn()));
    }

    static class FilterTestEventsFn extends DoFn<KV<String, Event>, KV<String, Event>> {
        private static final Logger LOG = LoggerFactory.getLogger(FilterTestEventsFn.class);

        @ProcessElement
        public void processElement(@Element KV<String, Event> element, OutputReceiver<KV<String, Event>> receiver) {
            try {
                Event event = element.getValue();
                EventParams params = event != null ? event.getEventParams() : null;
                boolean isTest = params != null && Boolean.TRUE.equals(params.getTestEvent());
                if (!isTest) {
                    receiver.output(element);
                }
            } catch (Exception e) {
                LOG.warn("Dropping event due to parse error: {}", e.getMessage());
            }
        }
    }
}
