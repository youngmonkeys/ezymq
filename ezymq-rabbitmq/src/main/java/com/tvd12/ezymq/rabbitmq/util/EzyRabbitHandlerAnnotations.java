package com.tvd12.ezymq.rabbitmq.util;

import com.tvd12.ezymq.rabbitmq.annotation.EzyRabbitHandler;

public final class EzyRabbitHandlerAnnotations {

    private EzyRabbitHandlerAnnotations() {}

    public static String getCommand(Object requestHandler) {
        EzyRabbitHandler anno = requestHandler
            .getClass()
            .getAnnotation(EzyRabbitHandler.class);
        return getCommand(anno);
    }

    public static String getCommand(EzyRabbitHandler annotation) {
        String cmd = annotation.value();
        if (cmd.isEmpty()) {
            cmd = annotation.command();
        }
        return cmd;
    }
}
