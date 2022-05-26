package com.tvd12.ezymq.activemq.util;

import com.tvd12.ezymq.activemq.annotation.EzyActiveHandler;

public final class EzyActiveHandlerAnnotations {

    private EzyActiveHandlerAnnotations() {}

    public static String getCommand(Object requestHandler) {
        EzyActiveHandler anno = requestHandler
            .getClass()
            .getAnnotation(EzyActiveHandler.class);
        return getCommand(anno);
    }

    public static String getCommand(EzyActiveHandler annotation) {
        String cmd = annotation.value();
        if (cmd.isEmpty()) {
            cmd = annotation.cmd();
        }
        return cmd;
    }
}
