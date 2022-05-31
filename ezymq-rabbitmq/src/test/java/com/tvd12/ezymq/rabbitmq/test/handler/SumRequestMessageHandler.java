package com.tvd12.ezymq.rabbitmq.test.handler;

import com.tvd12.ezymq.common.handler.EzyMQMessageConsumer;
import com.tvd12.ezymq.rabbitmq.annotation.EzyRabbitConsumer;
import com.tvd12.ezymq.rabbitmq.test.request.SumRequest;

@EzyRabbitConsumer(
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
