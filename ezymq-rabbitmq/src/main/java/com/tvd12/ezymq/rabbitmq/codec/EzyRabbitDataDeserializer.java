package com.tvd12.ezymq.rabbitmq.codec;

public interface EzyRabbitDataDeserializer {

    Object deserialize(String cmd, byte[] request);
}
