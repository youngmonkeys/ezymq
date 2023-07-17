package com.tvd12.ezymq.mosquitto.handler;

import com.tvd12.ezymq.mosquitto.endpoint.EzyMosquittoMessage;
import com.tvd12.ezymq.mosquitto.util.EzyMosquittoProperties;

public interface EzyMosquittoRpcCallHandler {

    void handleFire(
        EzyMosquittoProperties requestProperties,
        byte[] requestBody
    );

    default void handleFire(EzyMosquittoMessage request) {
        handleFire(
            request.getProperties(),
            request.getBody()
        );
    }

    byte[] handleCall(
        EzyMosquittoProperties requestProperties,
        byte[] requestBody,
        EzyMosquittoProperties.Builder replyPropertiesBuilder
    );

    default byte[] handleCall(
        EzyMosquittoMessage request,
        EzyMosquittoProperties.Builder replyPropertiesBuilder
    ) {
        return handleCall(
            request.getProperties(),
            request.getBody(),
            replyPropertiesBuilder
        );
    }
}
