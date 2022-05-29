package com.tvd12.ezymq.activemq.test.handler;

import com.tvd12.ezymq.activemq.annotation.EzyActiveHandler;
import com.tvd12.ezymq.activemq.handler.EzyActiveRequestHandler;
import com.tvd12.ezymq.activemq.test.request.SumRequest;
import com.tvd12.ezymq.activemq.test.response.SumResponse;

@EzyActiveHandler("sum")
public class SumRequestHandler implements EzyActiveRequestHandler<SumRequest> {

    @Override
    public Object handle(SumRequest request) {
        return new SumResponse(
            request.getA() + request.getB()
        );
    }
}
