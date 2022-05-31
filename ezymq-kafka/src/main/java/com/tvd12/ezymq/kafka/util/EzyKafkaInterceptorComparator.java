package com.tvd12.ezymq.kafka.util;

import com.tvd12.ezymq.kafka.annotation.EzyKafkaInterceptor;

import java.util.Comparator;

public final class EzyKafkaInterceptorComparator
    implements Comparator<Object> {

    private static final EzyKafkaInterceptorComparator INSTANCE =
        new EzyKafkaInterceptorComparator();

    public static EzyKafkaInterceptorComparator getInstance() {
        return INSTANCE;
    }

    @Override
    public int compare(Object a, Object b) {
        EzyKafkaInterceptor annoA =
            a.getClass().getAnnotation(EzyKafkaInterceptor.class);
        EzyKafkaInterceptor annoB =
            b.getClass().getAnnotation(EzyKafkaInterceptor.class);
        int priorityA = annoA != null ? annoA.priority() : 0;
        int priorityB = annoB != null ? annoB.priority() : 0;
        return Integer.compare(priorityA, priorityB);
    }
}
