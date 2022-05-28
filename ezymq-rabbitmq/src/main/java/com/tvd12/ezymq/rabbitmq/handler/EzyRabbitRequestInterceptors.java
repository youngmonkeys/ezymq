package com.tvd12.ezymq.rabbitmq.handler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class EzyRabbitRequestInterceptors implements EzyRabbitRequestInterceptor {

    private final List<EzyRabbitRequestInterceptor> interceptors =
        new ArrayList<>();

    public void addInterceptor(EzyRabbitRequestInterceptor interceptor) {
        this.interceptors.add(interceptor);
    }

    public void addInterceptors(Collection<EzyRabbitRequestInterceptor> interceptors) {
        this.interceptors.addAll(interceptors);
    }

    @Override
    public void preHandle(String cmd, Object message) {
        for (EzyRabbitRequestInterceptor interceptor : interceptors) {
            interceptor.preHandle(cmd, message);
        }
    }

    @Override
    public void postHandle(String cmd, Object message, Object result) {
        for (EzyRabbitRequestInterceptor interceptor : interceptors) {
            interceptor.postHandle(cmd, message, result);
        }
    }

    @Override
    public void postHandle(String cmd, Object message, Throwable e) {
        for (EzyRabbitRequestInterceptor interceptor : interceptors) {
            interceptor.postHandle(cmd, message, e);
        }
    }
}
