package com.tvd12.ezymq.common.test.bean_and_binding;

import com.tvd12.ezymq.common.test.annotation.EzyTestHandler;
import com.tvd12.ezymq.common.test.handler.EzyTestMQRequestHandler;

@EzyTestHandler(command = "test")
public class HelloRequestHandler
    implements EzyTestMQRequestHandler<HelloRequest> {

    @Override
    public Object handle(HelloRequest request) throws Exception {
        return request;
    }

    @Override
    public Class<?> getRequestType() {
        return HelloRequest.class;
    }
}
