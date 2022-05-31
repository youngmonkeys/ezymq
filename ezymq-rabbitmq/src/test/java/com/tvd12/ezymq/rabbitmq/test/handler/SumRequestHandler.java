package com.tvd12.ezymq.rabbitmq.test.handler;

import com.tvd12.ezymq.rabbitmq.annotation.EzyRabbitHandler;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitRequestHandler;
import com.tvd12.ezymq.rabbitmq.test.request.SumRequest;
import com.tvd12.ezymq.rabbitmq.test.response.SumResponse;

@EzyRabbitHandler("sum")
public class SumRequestHandler
    implements EzyRabbitRequestHandler<SumRequest> {

    @Override
    public Object handle(SumRequest request) {
        return new SumResponse(
            request.getA() + request.getB()
        );
    }
}
