package com.tvd12.ezymq.kafka.test.handler;

import com.tvd12.ezymq.kafka.annotation.EzyKafkaHandler;
import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageHandler;
import com.tvd12.ezymq.kafka.test.request.MultiplyRequest;
import com.tvd12.ezymq.kafka.test.request.SumRequest;

@EzyKafkaHandler(
    topic = "test",
    command = "multi"
)
public class MultiplyRequestHandler
    implements EzyKafkaMessageHandler<MultiplyRequest> {

    @Override
    public void process(MultiplyRequest message) {
        int result = message.getA() * message.getB();
        System.out.println("multi result: " + result);
    }
}
