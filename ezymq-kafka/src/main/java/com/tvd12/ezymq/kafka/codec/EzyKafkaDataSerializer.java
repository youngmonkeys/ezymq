package com.tvd12.ezymq.kafka.codec;

public interface EzyKafkaDataSerializer {

    byte[] serialize(Object data);
}
