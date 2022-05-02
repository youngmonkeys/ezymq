package com.tvd12.ezymq.activemq.codec;

public interface EzyActiveDataDeserializer {

    Object deserialize(String cmd, byte[] request);
}
