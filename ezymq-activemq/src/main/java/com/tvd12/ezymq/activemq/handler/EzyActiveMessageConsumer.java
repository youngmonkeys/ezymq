package com.tvd12.ezymq.activemq.handler;

public interface EzyActiveMessageConsumer<T> {
	
	void consume(T message);
	
}
