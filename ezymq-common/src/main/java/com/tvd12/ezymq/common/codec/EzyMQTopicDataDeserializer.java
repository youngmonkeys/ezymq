package com.tvd12.ezymq.common.codec;

public interface EzyMQTopicDataDeserializer {

    Object deserializeTopicMessage(String topic, String cmd, byte[] message);
}
