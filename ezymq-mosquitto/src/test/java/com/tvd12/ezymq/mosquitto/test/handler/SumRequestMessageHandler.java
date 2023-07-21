package com.tvd12.ezymq.mosquitto.test.handler;

import com.tvd12.ezymq.mosquitto.annotation.EzyMosquittoConsumer;
import com.tvd12.ezymq.mosquitto.test.request.SumRequest;
import com.tvd12.ezymq.common.handler.EzyMQMessageConsumer;

@EzyMosquittoConsumer(
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
