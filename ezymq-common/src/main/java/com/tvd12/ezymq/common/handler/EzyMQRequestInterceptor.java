package com.tvd12.ezymq.common.handler;

public interface EzyMQRequestInterceptor {

    void preHandle(String cmd, Object request);

    void postHandle(String cmd, Object request, Object response);

    void postHandle(String cmd, Object requestData, Throwable e);
}
