package com.tvd12.ezymq.kafka.test;

import com.tvd12.ezymq.kafka.EzyKafkaProxy;

public class SumConsumerProgram extends KafkaBaseTest {

    public static void main(String[] args) {
        EzyKafkaProxy.builder()
            .scan("com.tvd12.ezymq.kafka.test")
            .build();
    }
}
