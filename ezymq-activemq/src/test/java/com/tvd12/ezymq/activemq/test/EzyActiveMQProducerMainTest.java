package com.tvd12.ezymq.activemq.test;

import com.tvd12.ezyfox.util.EzyThreads;
import com.tvd12.ezymq.activemq.EzyActiveMQProxy;
import com.tvd12.ezymq.activemq.EzyActiveRpcProducer;
import com.tvd12.ezymq.activemq.EzyActiveTopic;
import com.tvd12.ezymq.activemq.test.request.SumRequest;
import com.tvd12.ezymq.activemq.test.response.SumResponse;

public class EzyActiveMQProducerMainTest {

    public static void main(String[] args) {
        EzyActiveMQProxy proxy = EzyActiveMQProxy.builder()
            .scan("com.tvd12.ezymq.activemq.test")
            .mapTopicMessageType("hello", "hello", SumRequest.class)
            .build();

        EzyActiveTopic<SumRequest> sumTopic = proxy.getTopic("hello");
        int a = 0;
        int b = 0;
        while (a < Integer.MAX_VALUE / 2) {
            sumTopic.publish("hello", new SumRequest(a, b));
            sumTopic.publish("sumMessage", new SumRequest(a, b));
            a ++;
            b ++;
            EzyThreads.sleep(1000);
        }
    }
}
