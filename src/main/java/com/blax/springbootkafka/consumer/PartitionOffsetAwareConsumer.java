package com.blax.springbootkafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.ConsumerAwareMessageListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Map;

@Component
public class PartitionOffsetAwareConsumer implements ConsumerSeekAware, ConsumerAwareMessageListener<String, EventEnvelope> {

    @Autowired
    PartitionOffsetStorage partitionOffsetStorage;

    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionOffsetAwareConsumer.class);

    @Value("com.blax.kafka.topic")
    private String topic;

    @Override
    public void registerSeekCallback(ConsumerSeekCallback callback) {
//        somehow onPartitionsAssigned works with none group as well, do not need callback.seek()
//        partitionOffsetStorage.getPartitionOffsetMap().forEach((partition, offset) -> {
//            LOGGER.info("Register Seek Callback, seek partition: [{}] ;offset: [{}]", partition, offset);
//            callback.seek(topic, partition, offset);
//        });
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        LOGGER.info("onPartitionsAssigned [" + assignments.toString() + "]");
        assignments.forEach((topicPartition, offset) -> {
            // on partition assigned, should fully honour the partition offset
            partitionOffsetStorage.reset(topicPartition.partition(), offset);
        });
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

    }

    @Override
    @KafkaListener(topicPartitions = @org.springframework.kafka.annotation.TopicPartition(topic = "${com.blax.kafka.topic}",
            partitions = "#{@finder.partitions('${com.blax.kafka.topic}')}"), concurrency = "3", groupId = "blax")
    public void onMessage(ConsumerRecord<String, EventEnvelope> data, Consumer<?, ?> consumer) {

//        //initialize partition offset map here due to the partitions and offset information only available after poll()
//        if (!partitionOffsetStorage.isSynced()) {
//            consumer.assignment().forEach(topicPartition -> {
//                long initialOffset = getInitialOffset(data, consumer, topicPartition);
//                partitionOffsetStorage.update(topicPartition.partition(), initialOffset);
//            });
//            partitionOffsetStorage.setSynced(!partitionOffsetStorage.getPartitionOffsetMap().isEmpty());
//        }

        if (isUnprocessedRecord(data)) {
            processRecord(data);
            partitionOffsetStorage.update(data.partition(), data.offset() + 1);
        } else {
            LOGGER.debug("Skipping this consumer record, as this event record has been processed before. Partition: {}, Offset: {}",
                    data.partition(), data.offset());
        }
    }

    protected void processRecord(ConsumerRecord<String, EventEnvelope> data) {
        EventEnvelope eventEnvelope = data.value();
        LOGGER.info("Received Event [" + eventEnvelope.toString() + "]");
    }

    private long getInitialOffset(ConsumerRecord<String, EventEnvelope> data, Consumer<?, ?> consumer,
                                  TopicPartition topicPartition) {
        //need to compare the consumer's position against the record's offset, as there could be cases whereby
        //the consumer is having more latest offset but not yet be consumed.
        return data.partition() == topicPartition.partition() ? data.offset() : consumer.position(topicPartition);
    }

    private boolean isUnprocessedRecord(ConsumerRecord<String, EventEnvelope> data) {
        return data.offset() >= partitionOffsetStorage.getPartitionOffsetMap().get(data.partition());
    }
}
