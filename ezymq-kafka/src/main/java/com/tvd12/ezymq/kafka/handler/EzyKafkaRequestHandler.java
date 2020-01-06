package com.tvd12.ezymq.kafka.handler;

public interface EzyKafkaRequestHandler<I, O> {
	
    O handle(I request) throws Exception;
    
}
