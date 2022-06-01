package com.tvd12.ezymq.kafka.test.handler;

import com.tvd12.ezymq.kafka.annotation.EzyKafkaHandler;
import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageHandler;
import com.tvd12.ezymq.kafka.test.request.SumRequest;

@EzyKafkaHandler(
    topic = "test",
    command = "sum"
)
public class SumRequestHandler
    implements EzyKafkaMessageHandler<SumRequest> {

    @Override
    public void process(SumRequest message) {
        int result = message.getA() + message.getB();
        System.out.println("sum result: " + result);
    }
}
