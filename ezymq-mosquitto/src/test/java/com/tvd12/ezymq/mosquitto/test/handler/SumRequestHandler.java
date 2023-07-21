package com.tvd12.ezymq.mosquitto.test.handler;

import com.tvd12.ezymq.mosquitto.annotation.EzyMosquittoHandler;
import com.tvd12.ezymq.mosquitto.handler.EzyMosquittoRequestHandler;
import com.tvd12.ezymq.mosquitto.test.request.SumRequest;
import com.tvd12.ezymq.mosquitto.test.response.SumResponse;

@EzyMosquittoHandler("sum")
public class SumRequestHandler implements EzyMosquittoRequestHandler<SumRequest> {

    @Override
    public Object handle(SumRequest request) {
        return new SumResponse(
            request.getA() + request.getB()
        );
    }
}
