package com.tvd12.ezymq.rabbitmq.codec;

public interface EzyRabbitDataSerializer {

	byte[] serialize(Object data);
	
}
