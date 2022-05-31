package com.tvd12.ezymq.activemq.test.handler;

import com.tvd12.ezymq.activemq.annotation.EzyActiveConsumer;
import com.tvd12.ezymq.activemq.test.request.SumRequest;
import com.tvd12.ezymq.common.handler.EzyMQMessageConsumer;

@EzyActiveConsumer(
    topic = "hello",
    command = "sumMessage"
)
public class SumRequestMessageHandler
    implements EzyMQMessageConsumer<SumRequest> {

    @Override
    public void consume(SumRequest message) {
        System.out.println("sum message: " + message);
    }
}
