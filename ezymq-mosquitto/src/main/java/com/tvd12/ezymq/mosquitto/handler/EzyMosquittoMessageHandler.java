package com.tvd12.ezymq.mosquitto.handler;

import com.tvd12.ezymq.mosquitto.util.EzyMosquittoProperties;

public interface EzyMosquittoMessageHandler {

    void handle(
        EzyMosquittoProperties properties,
        byte[] messageBody
    );
}
