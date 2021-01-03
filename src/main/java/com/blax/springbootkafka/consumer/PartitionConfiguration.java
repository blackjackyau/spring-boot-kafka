package com.blax.springbootkafka.consumer;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.kafka.core.ConsumerFactory;

/**
 * Defines partition related configuration.
 */
@Configuration
@Lazy
public class PartitionConfiguration {

    /**
     * Returns partition finder.
     * @param consumerFactory consumer factory
     * @return partition finder
     */
    @Bean
    public PartitionFinder finder(ConsumerFactory<String, String> consumerFactory) {
        return new PartitionFinder(consumerFactory);
    }

    /**
     * To find all the partitions that assigned to a consumer.
     */
    @RequiredArgsConstructor
    public static class PartitionFinder {

        private final ConsumerFactory<String, String> consumerFactory;

        /**
         * Returns all the partitions that assigned to the consumer by the given topic.
         * @param topic topic for the partitions
         * @return partitions for the topic
         */
        public String[] partitions(String topic) {
            try (Consumer<String, String> consumer = consumerFactory.createConsumer()) {
                return consumer.partitionsFor(topic).stream()
                        .map(pi -> String.valueOf(pi.partition()))
                        .toArray(String[]::new);
            }
        }
    }
}
