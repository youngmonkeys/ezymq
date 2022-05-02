package com.tvd12.ezymq.activemq.handler;

public interface EzyActiveExceptionInterceptor {

    void intercept(String cmd, Object requestData, Exception e);
}
