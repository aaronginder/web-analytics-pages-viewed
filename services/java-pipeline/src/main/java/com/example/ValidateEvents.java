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

/**
 * Validates events for required fields:
 * - event_params.test_event must exist and be boolean (true/false).
 * - event_params.page_title must exist (non-empty string).
 * Invalid events are dropped with a warning.
 */
public class ValidateEvents extends PTransform<PCollection<KV<String, Event>>, PCollection<KV<String, Event>>> {

    private static final Logger LOG = LoggerFactory.getLogger(ValidateEvents.class);

    @Override
    public PCollection<KV<String, Event>> expand(PCollection<KV<String, Event>> input) {
        return input.apply("ValidateEvents", ParDo.of(new ValidateEventsFn()));
    }

    static class ValidateEventsFn extends DoFn<KV<String, Event>, KV<String, Event>> {

        @ProcessElement
        public void processElement(@Element KV<String, Event> element, OutputReceiver<KV<String, Event>> receiver) {
            try {
                Event event = element.getValue();
                if (event == null) {
                    LOG.warn("Dropping event: null payload");
                    return;
                }

                EventParams params = event.getEventParams();
                if (params == null) {
                    LOG.warn("Dropping event: missing event_params");
                    return;
                }

                Boolean testEvent = params.getTestEvent();
                if (testEvent == null) {
                    LOG.warn("Dropping event: missing event_params.test_event");
                    return;
                }

                String pageTitle = params.getPageTitle();
                if (pageTitle == null || pageTitle.isEmpty()) {
                    LOG.warn("Dropping event: missing or empty event_params.page_title");
                    return;
                }

                receiver.output(element);
            } catch (Exception e) {
                LOG.warn("Dropping event due to parse/validation error: {}", e.getMessage());
            }
        }
    }
}
