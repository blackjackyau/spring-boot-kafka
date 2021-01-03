package com.blax.springbootkafka.producer;

import com.blax.springbootkafka.consumer.EventEnvelope;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class EventEnvelopeSerializer implements Serializer<EventEnvelope> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private static Logger LOGGER = LoggerFactory.getLogger(EventEnvelopeSerializer.class);

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public byte[] serialize(String topic, EventEnvelope data) {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            retVal = objectMapper.writeValueAsString(data).getBytes();
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
        return retVal;
    }
}
