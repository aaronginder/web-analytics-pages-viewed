package com.example;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Function to decode Kafka messages - now returns String instead of JSONObject
public class DecodeMessageFn extends DoFn<KV<String, String>, KV<String, String>> {
    private static final Logger LOG = LoggerFactory.getLogger(DecodeMessageFn.class);

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