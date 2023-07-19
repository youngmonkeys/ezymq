package com.tvd12.ezymq.rabbitmq.handler;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Delivery;

public interface EzyRabbitRpcCallHandler {

    void handleFire(
        BasicProperties requestProperties,
        byte[] requestBody
    );

    default void handleFire(Delivery request) {
        handleFire(
            request.getProperties(),
            request.getBody()
        );
    }

    byte[] handleCall(
        BasicProperties requestProperties,
        byte[] requestBody,
        BasicProperties.Builder replyPropertiesBuilder
    );

    default byte[] handleCall(
        Delivery request,
        BasicProperties.Builder replyPropertiesBuilder
    ) {
        return handleCall(
            request.getProperties(),
            request.getBody(),
            replyPropertiesBuilder
        );
    }
}
