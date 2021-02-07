package com.blax.springbootkafka;

import com.blax.springbootkafka.consumer.EventEnvelope;
import com.blax.springbootkafka.consumer.PartitionOffsetStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.event.NonResponsiveConsumerEvent;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@RestController
public class AppController {

    @Value("${com.blax.kafka.topic}")
    private String topic;

    @Autowired
    KafkaTemplate<String, EventEnvelope> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry listenerRegistry;

    @Autowired
    PartitionOffsetStorage partitionOffsetStorage;

    @Autowired
    ProducerFactory producerFactory;

    @PostMapping(value="/events")
    public Map sendEvents(@RequestBody Map event){
        EventEnvelope envelope = new EventEnvelope("1", "event", event);
        ListenableFuture<SendResult<String, EventEnvelope>> kafkaResult = kafkaTemplate.send(topic, envelope);
        Map result = new HashMap();
        try {
            SendResult<String, EventEnvelope> sendResult = kafkaResult.get(60, TimeUnit.SECONDS);
            result.put("status", sendResult.toString());
        } catch (Exception ex) {
            result.put("status error", ex.getMessage());
        }
        return result;
    }

    @EventListener()
    public void eventHandler(NonResponsiveConsumerEvent event) {
        //When Kafka server is down, NonResponsiveConsumerEvent error is caught here.
        System.out.println("CAUGHT the event "+ event);
    }

    @GetMapping(value="/partitions")
    public Map getPartitionOffsets(){
        return partitionOffsetStorage.getPartitionOffsetMap();
    }

    // Simulate scenario where listener is reset, it could be due to client cert expiration etc
    @PostMapping(value="/restartKafka")
    public Map restartKafka(){
        listenerRegistry.stop();
        listenerRegistry.start();
        producerFactory.reset();
        return new HashMap();
    }
}
