package com.tvd12.ezymq.mosquitto.util;

import com.tvd12.ezymq.mosquitto.annotation.EzyMosquittoInterceptor;

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
        EzyMosquittoInterceptor annoA =
            a.getClass().getAnnotation(EzyMosquittoInterceptor.class);
        EzyMosquittoInterceptor annoB =
            b.getClass().getAnnotation(EzyMosquittoInterceptor.class);
        int priorityA = annoA != null ? annoA.priority() : 0;
        int priorityB = annoB != null ? annoB.priority() : 0;
        return Integer.compare(priorityA, priorityB);
    }
}
