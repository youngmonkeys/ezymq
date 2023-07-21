package com.tvd12.ezymq.mosquitto.util;

import com.tvd12.ezymq.common.annotation.EzyConsumerAnnotationProperties;
import com.tvd12.ezymq.mosquitto.annotation.EzyMosquittoConsumer;

public final class EzyMosquittoConsumerAnnotations {

    private EzyMosquittoConsumerAnnotations() {}

    public static EzyConsumerAnnotationProperties getProperties(
        Object messageConsumer
    ) {
        EzyMosquittoConsumer anno = messageConsumer
            .getClass()
            .getAnnotation(EzyMosquittoConsumer.class);
        return getProperties(anno);
    }

    public static EzyConsumerAnnotationProperties getProperties(
        EzyMosquittoConsumer annotation
    ) {
        return new EzyConsumerAnnotationProperties(
            annotation.topic(),
            annotation.command()
        );
    }
}
