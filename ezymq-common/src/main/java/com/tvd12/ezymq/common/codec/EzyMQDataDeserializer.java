package com.tvd12.ezymq.common.codec;

public interface EzyMQDataDeserializer {

    Object deserialize(String cmd, byte[] request);
}
