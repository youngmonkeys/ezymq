package com.tvd12.ezymq.kafka.handler;

public interface EzyKafkaMessageInterceptor {

    default void preHandle(String topic, String cmd, Object message) {}

    default void postHandle(String topic, String cmd, Object message, Object result) {}

    default void postHandle(String topic, String cmd, Object message, Throwable e) {}
}
