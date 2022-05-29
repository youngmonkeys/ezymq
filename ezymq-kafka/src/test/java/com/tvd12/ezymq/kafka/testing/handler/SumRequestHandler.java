package com.tvd12.ezymq.kafka.testing.handler;

import com.tvd12.ezymq.kafka.annotation.EzyKafkaHandler;
import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageHandler;
import com.tvd12.ezymq.kafka.testing.request.SumRequest;
import com.tvd12.ezymq.kafka.testing.response.SumResponse;

@EzyKafkaHandler(
    topic = "test",
    command = "sum"
)
public class SumRequestHandler implements EzyKafkaMessageHandler<SumRequest> {

    @Override
    public Object handle(SumRequest request) {
        Object response = new SumResponse(
            request.getA() + request.getB()
        );
        System.out.println("sum: " + response);
        return response;
    }
}
