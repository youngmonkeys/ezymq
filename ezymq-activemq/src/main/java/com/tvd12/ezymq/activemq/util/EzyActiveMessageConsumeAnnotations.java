package com.tvd12.ezymq.activemq.util;

import com.tvd12.ezymq.activemq.annotation.EzyActiveMessageConsume;

public final class EzyActiveMessageConsumeAnnotations {

    private EzyActiveMessageConsumeAnnotations() {}

    public static String getCommand(EzyActiveMessageConsume annotation) {
        String cmd = annotation.value();
        if (cmd.isEmpty()) {
            cmd = annotation.cmd();
        }
        return cmd;
    }
}
