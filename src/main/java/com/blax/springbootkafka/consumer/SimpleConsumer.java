package com.blax.springbootkafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.event.NonResponsiveConsumerEvent;
import org.springframework.stereotype.Component;

//@Component
public class SimpleConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleConsumer.class);

    @KafkaListener(topics = "${com.blax.kafka.topic}", groupId = "blax")
    public void onMessage(EventEnvelope eventEnvelope) {
        LOGGER.info("Simple Consumer msg: " + eventEnvelope.toString());
    }

}
