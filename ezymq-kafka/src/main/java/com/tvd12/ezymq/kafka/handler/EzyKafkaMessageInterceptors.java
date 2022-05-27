package com.tvd12.ezymq.kafka.handler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class EzyKafkaMessageInterceptors implements EzyKafkaMessageInterceptor {

    private final List<EzyKafkaMessageInterceptor> interceptors =
        new ArrayList<>();

    public void addInterceptor(EzyKafkaMessageInterceptor interceptor) {
        this.interceptors.add(interceptor);
    }

    public void addInterceptors(Collection<EzyKafkaMessageInterceptor> interceptors) {
        this.interceptors.addAll(interceptors);
    }

    @Override
    public void preHandle(String topic, String cmd, Object message) {
        for (EzyKafkaMessageInterceptor interceptor : interceptors) {
            interceptor.preHandle(topic, cmd, message);
        }
    }

    @Override
    public void postHandle(String topic, String cmd, Object message, Object result) {
        for (EzyKafkaMessageInterceptor interceptor : interceptors) {
            interceptor.postHandle(topic, cmd, message, result);
        }
    }

    @Override
    public void postHandle(String topic, String cmd, Object message, Throwable e) {
        for (EzyKafkaMessageInterceptor interceptor : interceptors) {
            interceptor.postHandle(topic, cmd, message, e);
        }
    }
}
