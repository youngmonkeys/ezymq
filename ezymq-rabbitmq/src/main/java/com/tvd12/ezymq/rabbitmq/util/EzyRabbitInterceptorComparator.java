package com.tvd12.ezymq.rabbitmq.util;

import com.tvd12.ezymq.rabbitmq.annotation.EzyRabbitInterceptor;

import java.util.Comparator;

public final class EzyRabbitInterceptorComparator
    implements Comparator<Object> {

    private static final EzyRabbitInterceptorComparator INSTANCE =
        new EzyRabbitInterceptorComparator();

    public static EzyRabbitInterceptorComparator getInstance() {
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
