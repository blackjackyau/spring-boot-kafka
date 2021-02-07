package com.blax.springbootkafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class PartitionOffsetStorage {

    private static Logger LOGGER = LoggerFactory.getLogger(PartitionOffsetStorage.class);

    private final Map<Integer, Long> partitionOffsetMap = new ConcurrentHashMap<>();

    public void reset(int partition, long offset) {
        partitionOffsetMap.put(partition, offset);
    }

    public void update(int partition, long offset) {
        Long existingOffset = partitionOffsetMap.get(partition);
        if (existingOffset == null) {
            partitionOffsetMap.put(partition, offset);
        } else {
            if (offset > existingOffset) {
                partitionOffsetMap.put(partition, offset);
            } else {
                LOGGER.warn("Skipped due to offset is lesser than current offset");
            }
        }
    }

    public Map<Integer, Long> getPartitionOffsetMap() {
        return partitionOffsetMap;
    }
}
