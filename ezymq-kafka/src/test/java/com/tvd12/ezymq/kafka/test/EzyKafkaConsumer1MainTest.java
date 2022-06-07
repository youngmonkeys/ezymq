package com.tvd12.ezymq.kafka.test;

import com.tvd12.ezymq.kafka.EzyKafkaProducer;
import com.tvd12.ezymq.kafka.EzyKafkaProxy;
import com.tvd12.ezymq.kafka.test.request.SumRequest;

public class EzyKafkaConsumer1MainTest extends KafkaBaseTest {

    public static void main(String[] args) throws Exception {
        System.setProperty("active_profiles", "consumer-1");
        EzyKafkaProxy.builder()
            .scan("com.tvd12.ezymq.kafka.test")
            .build();
    }
}
