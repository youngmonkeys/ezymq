package com.tvd12.ezymq.activemq.test;

import com.tvd12.ezymq.activemq.EzyActiveMQProxy;
import com.tvd12.ezymq.activemq.EzyActiveRpcProducer;
import com.tvd12.ezymq.activemq.EzyActiveTopic;
import com.tvd12.ezymq.activemq.test.request.SumRequest;
import com.tvd12.ezymq.activemq.test.response.SumResponse;

public class EzyActiveMQProxyMainTest {

    public static void main(String[] args) {
        EzyActiveMQProxy proxy = EzyActiveMQProxy.builder()
            .scan("com.tvd12.ezymq.activemq.test")
            .mapTopicMessageType("hello", "hello", SumRequest.class)
            .build();
        EzyActiveRpcProducer producer = proxy.getRpcProducer("test");
        SumResponse sumResponse = producer.call(
            "sum",
            new SumRequest(1, 2),
            SumResponse.class
        );
        System.out.println(sumResponse);

        EzyActiveTopic<SumRequest> sumTopic = proxy.getTopic("hello");
        sumTopic.addConsumer("hello", message ->
            System.out.println("sum request: " + message)
        );
        sumTopic.publish("hello", new SumRequest(1, 2));
        sumTopic.publish("sumMessage", new SumRequest(1, 2));
    }
}
