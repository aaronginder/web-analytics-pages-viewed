package com.example;

import java.util.Map;

import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PTransform that reads from a Kafka topic.
 */
public class ReadFromKafka extends PTransform<PBegin, PCollection<KV<String, String>>> {
    private static final Logger LOG = LoggerFactory.getLogger(ReadFromKafka.class);
    
    private final Map<String, Object> consumerConfig;
    private final String bootstrapServers;
    private final String topic;
    private final StartingOffset startingOffset;

    public enum StartingOffset {
        EARLIEST, LATEST
    }

    /**
     * Creates a transform that reads from Kafka.
     *
     * @param topic The Kafka topic to read from
     * @param bootstrapServers The Kafka bootstrap servers
     * @param consumerConfig Additional Kafka consumer configuration
     */
    public ReadFromKafka(String topic, String bootstrapServers, Map<String, Object> consumerConfig) {
        this(topic, bootstrapServers, consumerConfig, StartingOffset.LATEST);
    }

    /**
     * Creates a transform that reads from Kafka with the specified starting offset.
     *
     * @param topic The Kafka topic to read from
     * @param bootstrapServers The Kafka bootstrap servers
     * @param consumerConfig Additional Kafka consumer configuration
     * @param startingOffset Whether to start from the earliest or latest offset
     */
    public ReadFromKafka(String topic, String bootstrapServers, 
                         Map<String, Object> consumerConfig, StartingOffset startingOffset) {
        this.topic = topic;
        this.bootstrapServers = bootstrapServers;
        this.consumerConfig = consumerConfig;
        this.startingOffset = startingOffset;
        
        // Set the auto.offset.reset based on the startingOffset
        if (startingOffset == StartingOffset.EARLIEST) {
            this.consumerConfig.put("auto.offset.reset", "earliest");
        } else {
            this.consumerConfig.put("auto.offset.reset", "latest");
        }
    }

    @Override
    public PCollection<KV<String, String>> expand(PBegin input) {
        LOG.info("Reading from Kafka topic {} with starting offset {}", topic, startingOffset);
        
        return input.apply("ReadFromKafka", KafkaIO.<String, String>read()
                .withBootstrapServers(bootstrapServers)
                .withTopic(topic)
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withConsumerConfigUpdates(consumerConfig)
                .withoutMetadata());
    }
}