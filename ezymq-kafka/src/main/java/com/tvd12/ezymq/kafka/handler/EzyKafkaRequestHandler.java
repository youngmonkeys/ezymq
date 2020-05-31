package com.tvd12.ezymq.kafka.handler;

public interface EzyKafkaRequestHandler<I> {
	
    default Object handle(I request) throws Exception {
    	process(request);
    	return Boolean.TRUE;
    }
    
    default void process(I request) throws Exception {
    }
    
}
