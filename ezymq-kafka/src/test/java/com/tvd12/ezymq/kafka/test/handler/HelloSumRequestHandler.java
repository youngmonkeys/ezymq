package com.tvd12.ezymq.kafka.test.handler;

import com.tvd12.ezymq.kafka.annotation.EzyKafkaHandler;
import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageHandler;
import com.tvd12.ezymq.kafka.test.request.SumRequest;

@EzyKafkaHandler(
    topic = "hello",
    command = "sum"
)
public class HelloSumRequestHandler
    implements EzyKafkaMessageHandler<SumRequest> {

    @Override
    public void process(SumRequest message) {
        int result = message.getA() + message.getB();
        System.out.println("sum result: " + result);
    }
}
