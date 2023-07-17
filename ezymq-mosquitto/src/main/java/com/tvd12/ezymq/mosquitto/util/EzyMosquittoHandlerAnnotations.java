package com.tvd12.ezymq.mosquitto.util;

import com.tvd12.ezymq.mosquitto.annotation.EzyRabbitHandler;

public final class EzyMosquittoHandlerAnnotations {

    private EzyMosquittoHandlerAnnotations() {}

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
