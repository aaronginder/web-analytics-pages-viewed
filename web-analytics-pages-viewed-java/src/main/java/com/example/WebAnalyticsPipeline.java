package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;

public class WebAnalyticsPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(WebAnalyticsPipeline.class);

    public static void main(String[] args) {
        // Configure logging - add this at the top of main
        java.util.logging.Logger kafkaLogger = java.util.logging.Logger.getLogger("org.apache.kafka");
        kafkaLogger.setLevel(Level.WARNING);  // Only show WARNING and above

        // Set up pipeline options - use default runner instead of explicit DirectRunner
        PipelineOptions options = PipelineOptionsFactory.create();
        
        System.out.println("Starting Kafka consumer pipeline...");
        
        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);
        
        // Kafka consumer properties
        Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "web-analytics-group");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        // Build and run the pipeline
        pipeline
            .apply("ReadFromKafka", new ReadFromKafka("web-analytics-events", "localhost:29092", consumerConfig))
            .apply("DecodeMessages", ParDo.of(new DecodeMessageFn()))
            .apply("PrintMessages", MapElements.into(TypeDescriptors.voids())
                .via((KV<String, String> kv) -> {  // Changed JSONObject to String
                    System.out.println("Key: " + kv.getKey() + ", Value: " + kv.getValue());
                    return null;
                }));
        
        pipeline.run().waitUntilFinish();
    }

    // Function to decode Kafka messages - now returns String instead of JSONObject
    static class DecodeMessageFn extends DoFn<KV<String, String>, KV<String, String>> {
        @ProcessElement
        public void processElement(@Element KV<String, String> element, OutputReceiver<KV<String, String>> receiver) {
            String key = element.getKey();
            String value = element.getValue();
            
            try {
                // Parse to validate and prettify (optional)
                JSONObject jsonValue = new JSONObject(value);
                // Output the string representation
                receiver.output(KV.of(key, jsonValue.toString()));
            } catch (Exception e) {
                LOG.error("Error decoding message: {}", e.getMessage());
            }
        }
    }

    // This would be the implementation of the add_time_spent_on_page function
    // Not used in the current pipeline, but included for completeness
    static class AddTimeSpentOnPageFn extends DoFn<KV<String, Iterable<String>>, Iterable<JSONObject>> {
        @ProcessElement
        public void processElement(@Element KV<String, Iterable<String>> element, OutputReceiver<Iterable<JSONObject>> receiver) {
            // Implementation would go here - similar logic to your Python function
            // Sort events, calculate time differences, etc.
            String sessionId = UUID.randomUUID().toString();
            
            // Example of what the implementation would look like
            // Note: This is not a complete implementation
            /*
            List<JSONObject> parsedEvents = new ArrayList<>();
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
            
            receiver.output(results);
            */
        }
    }
}