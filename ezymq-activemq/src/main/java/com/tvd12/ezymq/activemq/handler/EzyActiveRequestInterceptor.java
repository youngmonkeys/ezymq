package com.tvd12.ezymq.activemq.handler;

public interface EzyActiveRequestInterceptor {

    void preHandle(String cmd, Object request);

    void postHandle(String cmd, Object request, Object response);

    void postHandle(String cmd, Object requestData, Throwable e);
}
