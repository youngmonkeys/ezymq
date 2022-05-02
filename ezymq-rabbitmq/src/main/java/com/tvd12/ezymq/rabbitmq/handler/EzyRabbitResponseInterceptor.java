package com.tvd12.ezymq.rabbitmq.handler;

public interface EzyRabbitResponseInterceptor {

    void intercept(String cmd, Object requestData, Object responseData);
}
