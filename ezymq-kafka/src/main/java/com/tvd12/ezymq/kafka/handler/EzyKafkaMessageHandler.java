package com.tvd12.ezymq.kafka.handler;

public interface EzyKafkaMessageHandler<I> {
	
    default Object handle(I message) throws Exception {
    	process(message);
    	return Boolean.TRUE;
    }
    
    default void process(I message) throws Exception {
    }
    
}
