package com.blax.springbootkafka;

import com.blax.springbootkafka.consumer.PartitionOffsetStorage;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.kafka.event.NonResponsiveConsumerEvent;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

// https://github.com/spring-projects/spring-kafka/issues/1243
@Component
class BrokerDetector {

    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerDetector.class);

    private final Map<String, Object> adminProps;

    @Autowired
    KafkaListenerEndpointRegistry listenerRegistry;

    @Autowired
    ProducerFactory producerFactory;

    BrokerDetector(KafkaAdmin admin, ConcurrentKafkaListenerContainerFactory<?, ?> factory) {
        factory.getContainerProperties().setIdleEventInterval(5000L);
        this.adminProps = new HashMap<>(admin.getConfigurationProperties());
        this.adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
    }

    // No longer useful https://github.com/spring-projects/spring-kafka/issues/1243
//    @EventListener()
//    public void eventHandler(NonResponsiveConsumerEvent event) {
//        //When Kafka server is down, NonResponsiveConsumerEvent error is caught here.
//        LOGGER.error("NonResponsiveConsumerEvent restart kafka"+ event);
//    }

    @EventListener
    public void idler(ListenerContainerIdleEvent event) {
        LOGGER.error("ListenerContainerIdleEvent"+ event);
        LOGGER.info(event.toString());
        try (AdminClient client = AdminClient.create(this.adminProps)) {
            String cluster = client.describeCluster().clusterId().get(10,  TimeUnit.SECONDS);
            LOGGER.info("Broker ok: " + cluster);
        }
        catch (Exception e) {
            LOGGER.error("Broker not available", e);
            LOGGER.info("Attempt to restart kafka client");
            listenerRegistry.stop();
            listenerRegistry.start();
            producerFactory.reset();

        }
    }

}