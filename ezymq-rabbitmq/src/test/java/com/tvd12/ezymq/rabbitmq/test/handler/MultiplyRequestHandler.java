package com.tvd12.ezymq.rabbitmq.test.handler;

import com.tvd12.ezymq.rabbitmq.annotation.EzyRabbitHandler;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitRequestHandler;
import com.tvd12.ezymq.rabbitmq.test.request.MultiplyRequest;

@EzyRabbitHandler(command = "multi")
public class MultiplyRequestHandler
    implements EzyRabbitRequestHandler<MultiplyRequest> {

    @Override
    public void process(MultiplyRequest message) {
        int result = message.getA() * message.getB();
        System.out.println("multi result: " + result);
    }
}
