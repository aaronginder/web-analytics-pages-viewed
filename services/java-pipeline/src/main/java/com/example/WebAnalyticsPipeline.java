package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

public class WebAnalyticsPipeline {
    public static void main(String[] args) {
        // Configure logging - add this at the top of main
        java.util.logging.Logger kafkaLogger = java.util.logging.Logger.getLogger("org.apache.kafka");
        kafkaLogger.setLevel(Level.WARNING);

        java.util.logging.Logger beamKafkaLogger = java.util.logging.Logger.getLogger("org.apache.beam.sdk.io.kafka");
        beamKafkaLogger.setLevel(Level.WARNING);

        // Set up pipeline options
        PipelineOptions options = PipelineOptionsFactory.create();

        System.out.println("Starting Kafka consumer pipeline...");

        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);

        // Kafka configuration
        String topic = "web-analytics-events";
        String bootstrapServers = "localhost:29092";

        // Kafka consumer properties
        Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "web-analytics-group");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Build and run the pipeline
        PCollection<Iterable<String>> input = pipeline
                .apply("ReadFromKafka", new ReadFromKafka(topic, bootstrapServers, consumerConfig))
                .apply("ValidateEvents", new ValidateEvents())
                .apply("FilterTestEvents", new FilterEvents())
                .apply("AggregateEvents", new AggregateEvents());

        // Print the messages to the console
        input
                .apply("PrintAggregatedEvents", MapElements.into(TypeDescriptors.voids())
                        .via((Iterable<String> events) -> {
                            for (String event : events) {
                                System.out.println(event);
                            }
                            return null;
                        }));

        pipeline.run().waitUntilFinish();
    }
}
