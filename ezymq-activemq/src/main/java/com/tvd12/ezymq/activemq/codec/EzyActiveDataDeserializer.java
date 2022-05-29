package com.tvd12.ezymq.activemq.codec;

@Deprecated
public interface EzyActiveDataDeserializer {

    Object deserialize(String cmd, byte[] request);
}
