package com.tvd12.ezymq.mosquitto.util;

import com.tvd12.ezymq.mosquitto.annotation.EzyMosquittoHandler;

public final class EzyMosquittoHandlerAnnotations {

    private EzyMosquittoHandlerAnnotations() {}

    public static String getCommand(Object requestHandler) {
        EzyMosquittoHandler anno = requestHandler
            .getClass()
            .getAnnotation(EzyMosquittoHandler.class);
        return getCommand(anno);
    }

    public static String getCommand(EzyMosquittoHandler annotation) {
        String cmd = annotation.value();
        if (cmd.isEmpty()) {
            cmd = annotation.command();
        }
        return cmd;
    }
}
