package com.tvd12.ezymq.activemq.handler;

import com.tvd12.ezymq.activemq.util.EzyActiveProperties;

public interface EzyActiveMessageHandler {

    void handle(
        EzyActiveProperties properties,
        byte[] messageBody
    );
}
