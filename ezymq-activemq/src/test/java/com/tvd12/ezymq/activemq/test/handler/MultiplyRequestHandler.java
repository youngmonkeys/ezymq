package com.tvd12.ezymq.activemq.test.handler;

import com.tvd12.ezymq.activemq.annotation.EzyActiveHandler;
import com.tvd12.ezymq.activemq.handler.EzyActiveRequestHandler;
import com.tvd12.ezymq.activemq.test.request.MultiplyRequest;

@EzyActiveHandler(command = "multi")
public class MultiplyRequestHandler
    implements EzyActiveRequestHandler<MultiplyRequest> {

    @Override
    public void process(MultiplyRequest message) {
        int result = message.getA() * message.getB();
        System.out.println("multi result: " + result);
    }
}
