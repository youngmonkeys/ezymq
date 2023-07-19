package com.tvd12.ezymq.mosquitto.util;

import com.tvd12.ezymq.mosquitto.annotation.EzyRabbitInterceptor;

import java.util.Comparator;

public final class EzyMosquittoInterceptorComparator
    implements Comparator<Object> {

    private static final EzyMosquittoInterceptorComparator INSTANCE =
        new EzyMosquittoInterceptorComparator();

    public static EzyMosquittoInterceptorComparator getInstance() {
        return INSTANCE;
    }

    @Override
    public int compare(Object a, Object b) {
        EzyRabbitInterceptor annoA =
            a.getClass().getAnnotation(EzyRabbitInterceptor.class);
        EzyRabbitInterceptor annoB =
            b.getClass().getAnnotation(EzyRabbitInterceptor.class);
        int priorityA = annoA != null ? annoA.priority() : 0;
        int priorityB = annoB != null ? annoB.priority() : 0;
        return Integer.compare(priorityA, priorityB);
    }
}
