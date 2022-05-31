package com.tvd12.ezymq.activemq.util;

import com.tvd12.ezymq.activemq.annotation.EzyActiveInterceptor;

import java.util.Comparator;

public final class EzyActiveInterceptorComparator
    implements Comparator<Object> {

    private static final EzyActiveInterceptorComparator INSTANCE =
        new EzyActiveInterceptorComparator();

    public static EzyActiveInterceptorComparator getInstance() {
        return INSTANCE;
    }

    @Override
    public int compare(Object a, Object b) {
        EzyActiveInterceptor annoA =
            a.getClass().getAnnotation(EzyActiveInterceptor.class);
        EzyActiveInterceptor annoB =
            b.getClass().getAnnotation(EzyActiveInterceptor.class);
        int priorityA = annoA != null ? annoA.priority() : 0;
        int priorityB = annoB != null ? annoB.priority() : 0;
        return Integer.compare(priorityA, priorityB);
    }
}
