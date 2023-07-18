package com.tvd12.ezymq.mosquitto.util;

import com.tvd12.ezymq.mosquitto.message.EzyMqttMqMessage;

public final class EzyMqttMessages {

    private EzyMqttMessages() {}

    public static EzyMqttMqMessage toMqttMqMessage(
        EzyMosquittoProperties properties,
        byte[] body
    ) {
        return EzyMqttMqMessage.builder()
            .id(properties.getMessageId())
            .type(properties.getMessageType())
            .headers(properties.getHeaders())
            .body(body)
            .qos(properties.getQos())
            .retained(properties.isRetained())
            .build();
    }
}
