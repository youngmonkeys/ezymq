package com.tvd12.ezymq.kafka.handler;

public interface EzyKafkaResponseInterceptor {
	
	void intercept(String cmd, Object requestData, Object responseData);
	
}
