package com.blax.springbootkafka.consumer;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class EventEnvelope {

    private String correlationId;
    private String messageType;
    private Object payload;

}
