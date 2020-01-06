package com.tvd12.ezymq.rabbitmq.handler;

public interface EzyRabbitRequestHandler<I, O> {
	
    O handle(I request) throws Exception;
    
}
