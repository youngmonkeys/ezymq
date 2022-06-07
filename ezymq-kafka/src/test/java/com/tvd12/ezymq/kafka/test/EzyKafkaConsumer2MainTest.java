package com.tvd12.ezymq.kafka.test;

import com.tvd12.ezymq.kafka.EzyKafkaProxy;

public class EzyKafkaConsumer2MainTest extends KafkaBaseTest {

    public static void main(String[] args) throws Exception {
        System.setProperty("active_profiles", "consumer-2");
        EzyKafkaProxy.builder()
            .scan("com.tvd12.ezymq.kafka.test")
            .build();
    }
}
