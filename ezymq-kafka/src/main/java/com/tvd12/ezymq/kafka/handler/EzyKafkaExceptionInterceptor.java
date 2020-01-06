package com.tvd12.ezymq.kafka.handler;

public interface EzyKafkaExceptionInterceptor {
	
	void intercept(String cmd, Object requestData, Exception e);
	
}
