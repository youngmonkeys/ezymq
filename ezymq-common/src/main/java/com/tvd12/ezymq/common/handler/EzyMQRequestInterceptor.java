package com.tvd12.ezymq.common.handler;

public interface EzyMQRequestInterceptor {

    default void preHandle(String cmd, Object request) {}

    default void postHandle(String cmd, Object request, Object response) {}

    default void postHandle(String cmd, Object requestData, Throwable e) {}
}
