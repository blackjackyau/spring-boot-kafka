package com.blax.springbootkafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.kafka.config.AbstractKafkaListenerContainerFactory;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.stereotype.Component;

import java.util.Collection;

// Initial idea is to hijack the bean creation lifecycle to inject the consumer re-balance listener, NOT GONNA work, incorrect way to hijack re-balance consumer
//@Component
//public class ConsumerInjector implements BeanPostProcessor {
//
//    @Autowired
//    PartitionOffsetStorage partitionOffsetStorage;
//
//    @Override
//    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
//        if (bean instanceof AbstractKafkaListenerContainerFactory) {
//            ((AbstractKafkaListenerContainerFactory<?, ?, ?>) bean).getContainerProperties()
//                    .setConsumerRebalanceListener(new PartitionOffsetAwareConsumerListener(partitionOffsetStorage));
//        }
//        return bean;
//    }
//}

//class PartitionOffsetAwareConsumerListener implements ConsumerAwareRebalanceListener {
//
//    private PartitionOffsetStorage partitionOffsetStorage;
//
//    private static Logger LOGGER = LoggerFactory.getLogger(PartitionOffsetAwareConsumerListener.class);
//
//    public PartitionOffsetAwareConsumerListener(PartitionOffsetStorage partitionOffsetStorage) {
//        this.partitionOffsetStorage = partitionOffsetStorage;
//    }
//
//    @Override
//    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
//        partitions.forEach(topicPartition -> {
//            LOGGER.info("onPartitionsAssigned [" + topicPartition.toString() + "]");
//            partitionOffsetStorage.update(topicPartition.partition(), consumer.position(topicPartition));
//        });
//    }
//}