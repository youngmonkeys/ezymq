package com.tvd12.ezymq.mosquitto.test.handler;

import com.tvd12.ezymq.mosquitto.annotation.EzyMosquittoHandler;
import com.tvd12.ezymq.mosquitto.handler.EzyMosquittoRequestHandler;
import com.tvd12.ezymq.mosquitto.test.request.MultiplyRequest;

@EzyMosquittoHandler(command = "multi")
public class MultiplyRequestHandler
    implements EzyMosquittoRequestHandler<MultiplyRequest> {

    @Override
    public void process(MultiplyRequest message) {
        int result = message.getA() * message.getB();
        System.out.println("multi result: " + result);
    }
}
