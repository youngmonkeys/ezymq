package com.tvd12.ezymq.kafka.codec;

public interface EzyKafkaDataDeserializer {

	Object deserialize(String topic, String cmd, byte[] request);
	
}
