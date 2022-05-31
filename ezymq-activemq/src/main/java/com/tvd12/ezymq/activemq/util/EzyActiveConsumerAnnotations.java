package com.tvd12.ezymq.activemq.util;

import com.tvd12.ezymq.activemq.annotation.EzyActiveConsumer;
import com.tvd12.ezymq.common.annotation.EzyConsumerAnnotationProperties;

public final class EzyActiveConsumerAnnotations {

    private EzyActiveConsumerAnnotations() {}

    public static EzyConsumerAnnotationProperties getProperties(
        Object messageConsumer
    ) {
        EzyActiveConsumer anno = messageConsumer
            .getClass()
            .getAnnotation(EzyActiveConsumer.class);
        return getProperties(anno);
    }

    public static EzyConsumerAnnotationProperties getProperties(
        EzyActiveConsumer annotation
    ) {
        return new EzyConsumerAnnotationProperties(
            annotation.topic(),
            annotation.command()
        );
    }
}
