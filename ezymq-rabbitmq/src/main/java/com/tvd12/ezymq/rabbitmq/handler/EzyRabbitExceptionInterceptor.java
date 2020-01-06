package com.tvd12.ezymq.rabbitmq.handler;

public interface EzyRabbitExceptionInterceptor {
	
	void intercept(String cmd, Object requestData, Exception e);
	
}
