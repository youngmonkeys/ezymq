package com.tvd12.ezymq.activemq.test;

import com.tvd12.ezymq.activemq.EzyActiveMQProxy;
import com.tvd12.ezymq.activemq.EzyActiveTopic;
import com.tvd12.ezymq.activemq.test.request.SumRequest;

public class EzyActiveMQConsumer2MainTest {

    public static void main(String[] args) {
        EzyActiveMQProxy proxy = EzyActiveMQProxy.builder()
            .scan("com.tvd12.ezymq.activemq.test")
            .mapTopicMessageType("hello", "hello", SumRequest.class)
            .build();

        EzyActiveTopic<SumRequest> sumTopic = proxy.getTopic("hello");
        sumTopic.addConsumer("hello", message ->
            System.out.println("sum request: " + message)
        );
    }
}
