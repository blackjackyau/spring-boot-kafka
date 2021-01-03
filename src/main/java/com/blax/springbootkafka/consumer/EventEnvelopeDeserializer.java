package com.blax.springbootkafka.consumer;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class EventEnvelopeDeserializer implements Deserializer<EventEnvelope> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger LOGGER = LoggerFactory.getLogger(EventEnvelopeDeserializer.class);

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public EventEnvelope deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, EventEnvelope.class);
        } catch (IOException e) {
            LOGGER.warn("Failed to deserialise event envelope", e); // NOI18N
            return null;
        }
    }
}
