package com.tvd12.ezymq.common.handler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class EzyMQRequestInterceptors<I extends EzyMQRequestInterceptor>
    implements EzyMQRequestInterceptor {

    protected final List<I> interceptors =
        new ArrayList<>();

    public void addInterceptor(I interceptor) {
        this.interceptors.add(interceptor);
    }

    public void addInterceptors(Collection<I> interceptors) {
        this.interceptors.addAll(interceptors);
    }

    @Override
    public void preHandle(String cmd, Object message) {
        for (I interceptor : interceptors) {
            interceptor.preHandle(cmd, message);
        }
    }

    @Override
    public void postHandle(String cmd, Object message, Object result) {
        for (EzyMQRequestInterceptor interceptor : interceptors) {
            interceptor.postHandle(cmd, message, result);
        }
    }

    @Override
    public void postHandle(String cmd, Object message, Throwable e) {
        for (EzyMQRequestInterceptor interceptor : interceptors) {
            interceptor.postHandle(cmd, message, e);
        }
    }
}
