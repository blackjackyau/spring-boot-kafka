package com.blax.springbootkafka;

import com.blax.springbootkafka.consumer.EventEnvelope;
import com.blax.springbootkafka.consumer.PartitionOffsetStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

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

    @PostMapping(value="/events")
    public EventEnvelope sendEvents(@RequestBody Map event){
        EventEnvelope envelope = new EventEnvelope("1", "event", event);
        kafkaTemplate.send(topic, envelope);
        return envelope;
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
        return new HashMap();
    }
}
