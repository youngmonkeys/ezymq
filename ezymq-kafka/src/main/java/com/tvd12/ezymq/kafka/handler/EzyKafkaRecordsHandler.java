package com.tvd12.ezymq.kafka.handler;

import org.apache.kafka.clients.consumer.ConsumerRecord;

@SuppressWarnings("rawtypes")
public interface EzyKafkaRecordsHandler {

    void handleRecord(ConsumerRecord record);
}
