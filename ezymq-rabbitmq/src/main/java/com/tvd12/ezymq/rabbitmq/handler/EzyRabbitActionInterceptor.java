package com.tvd12.ezymq.rabbitmq.handler;

public interface EzyRabbitActionInterceptor extends
		EzyRabbitRequestInterceptor, 
		EzyRabbitResponseInterceptor, 
		EzyRabbitExceptionInterceptor{

}
