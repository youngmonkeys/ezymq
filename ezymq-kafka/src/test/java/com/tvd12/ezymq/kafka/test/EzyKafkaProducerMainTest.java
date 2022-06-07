package com.tvd12.ezymq.kafka.test;

import com.tvd12.ezyfox.util.EzyThreads;
import com.tvd12.ezymq.kafka.EzyKafkaProducer;
import com.tvd12.ezymq.kafka.EzyKafkaProxy;
import com.tvd12.ezymq.kafka.test.request.SumRequest;

public class EzyKafkaProducerMainTest extends KafkaBaseTest {

    public static void main(String[] args) throws Exception {
        System.setProperty("active_profiles", "producer");
        EzyKafkaProxy kafkaContext = EzyKafkaProxy.builder()
            .scan("com.tvd12.ezymq.kafka.test")
            .build();
        int a = 0;
        int b = 0;
        EzyKafkaProducer consumer = kafkaContext.getProducer("hello");
        while (a < Integer.MAX_VALUE / 2) {
            consumer.send("sum", new SumRequest(a++, b++));
            EzyThreads.sleep(1000);
        }
    }
}
