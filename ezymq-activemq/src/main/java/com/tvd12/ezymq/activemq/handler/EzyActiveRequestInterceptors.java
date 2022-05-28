package com.tvd12.ezymq.activemq.handler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class EzyActiveRequestInterceptors implements EzyActiveRequestInterceptor {

    private final List<EzyActiveRequestInterceptor> interceptors =
        new ArrayList<>();

    public void addInterceptor(EzyActiveRequestInterceptor interceptor) {
        this.interceptors.add(interceptor);
    }

    public void addInterceptors(Collection<EzyActiveRequestInterceptor> interceptors) {
        this.interceptors.addAll(interceptors);
    }

    @Override
    public void preHandle(String cmd, Object message) {
        for (EzyActiveRequestInterceptor interceptor : interceptors) {
            interceptor.preHandle(cmd, message);
        }
    }

    @Override
    public void postHandle(String cmd, Object message, Object result) {
        for (EzyActiveRequestInterceptor interceptor : interceptors) {
            interceptor.postHandle(cmd, message, result);
        }
    }

    @Override
    public void postHandle(String cmd, Object message, Throwable e) {
        for (EzyActiveRequestInterceptor interceptor : interceptors) {
            interceptor.postHandle(cmd, message, e);
        }
    }
}
