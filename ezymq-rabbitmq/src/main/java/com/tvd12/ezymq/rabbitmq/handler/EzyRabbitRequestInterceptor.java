package com.tvd12.ezymq.rabbitmq.handler;

public interface EzyRabbitRequestInterceptor {

    void preHandle(String cmd, Object request);

    void postHandle(String cmd, Object request, Object response);

    void postHandle(String cmd, Object request, Throwable e);
}
