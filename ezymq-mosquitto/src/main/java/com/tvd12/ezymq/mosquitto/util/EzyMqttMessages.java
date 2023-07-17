package com.tvd12.ezymq.mosquitto.util;

import static com.tvd12.ezyfox.io.EzyStrings.EMPTY_STRING;

import org.eclipse.paho.client.mqttv3.MqttMessage;

import com.tvd12.ezyfox.io.EzyBytes;
import com.tvd12.ezyfox.io.EzyInts;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMosquittoMessage;

public final class EzyMqttMessages {

    private EzyMqttMessages() {}

    public static MqttMessage toMqttMessage(
        boolean isRpcTopic,
        EzyMosquittoProperties properties,
        byte[] body
    ) {
        MqttMessage mqttMessage = new MqttMessage();
        mqttMessage.setId(properties.getMessageId());
        mqttMessage.setPayload(
            packMessagePayload(
                isRpcTopic,
                properties.getMessageType(),
                body
            )
        );
        mqttMessage.setQos(properties.getQos());
        mqttMessage.setRetained(properties.isRetained());
        return mqttMessage;
    }

    public static EzyMosquittoMessage toMessage(
        boolean isRpcTopic,
        EzyMosquittoProperties properties,
        byte[] body
    ) {
        String messageType = "";
        byte[] messageBody = body;
        EzyMosquittoProperties actualProperties = properties;
        if (isRpcTopic) {
            if (body.length > 2) {
                byte[] messageTypeLengthBytes = EzyBytes.copy(body, 0, 2);
                int messageTypeLength = EzyInts.bin2int(messageTypeLengthBytes);
                int nextLength = 2 + messageTypeLength;
                if (body.length > nextLength) {
                    byte[] messageTypeBytes = EzyBytes.copy(body, 2, messageTypeLength);
                    messageType = new String(messageTypeBytes);
                    messageBody = EzyBytes.copy(
                        body,
                        nextLength,
                        body.length - nextLength
                    );
                    actualProperties = properties
                        .toBuilder()
                        .messageType(messageType)
                        .build();
                }
            }
        }
        return new EzyMosquittoMessage(
            actualProperties,
            messageBody
        );
    }

    private static byte[] packMessagePayload(
        boolean isRpcTopic,
        String messageType,
        byte[] body
    ) {
        if (isRpcTopic) {
            String type = messageType == null
                          ? EMPTY_STRING
                          : messageType;
            return EzyBytes.merge(
                new byte[][] {
                    EzyBytes.getBytes((short) type.length()),
                    type.getBytes(),
                    body
                }
            );
        }
        return body;
    }
}
