package com.tvd12.ezymq.rabbitmq.util;

import com.tvd12.ezymq.common.annotation.EzyConsumerAnnotationProperties;
import com.tvd12.ezymq.rabbitmq.annotation.EzyRabbitConsumer;

public final class EzyRabbitConsumerAnnotations {

    private EzyRabbitConsumerAnnotations() {}

    public static EzyConsumerAnnotationProperties getProperties(
        Object messageConsumer
    ) {
        EzyRabbitConsumer anno = messageConsumer
            .getClass()
            .getAnnotation(EzyRabbitConsumer.class);
        return getProperties(anno);
    }

    public static EzyConsumerAnnotationProperties getProperties(
        EzyRabbitConsumer annotation
    ) {
        return new EzyConsumerAnnotationProperties(
            annotation.topic(),
            annotation.command()
        );
    }
}
