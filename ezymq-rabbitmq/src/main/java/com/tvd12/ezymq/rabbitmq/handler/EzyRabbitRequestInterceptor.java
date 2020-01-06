package com.tvd12.ezymq.rabbitmq.handler;

public interface EzyRabbitRequestInterceptor {
	
	void intercept(String cmd, Object requestData);
	
}
