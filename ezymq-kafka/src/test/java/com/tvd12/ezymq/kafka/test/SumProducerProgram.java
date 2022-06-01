package com.tvd12.ezymq.kafka.test;

import com.tvd12.ezymq.kafka.EzyKafkaProducer;
import com.tvd12.ezymq.kafka.EzyKafkaProxy;
import com.tvd12.ezymq.kafka.test.request.SumRequest;

public class SumProducerProgram extends KafkaBaseTest {

    public static void main(String[] args) {
        EzyKafkaProxy kafkaContext = EzyKafkaProxy.builder()
            .scan("com.tvd12.ezymq.kafka.test")
            .build();
        EzyKafkaProducer consumer = kafkaContext.getProducer("test");
        consumer.send("sum", new SumRequest(1, 2));
    }
}
