package com.tvd12.ezymq.rabbitmq.handler;

public interface EzyRabbitMessageConsumer<T> {
	
	void consume(T message);
	
}
