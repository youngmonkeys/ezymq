package com.tvd12.ezymq.kafka.handler;

public interface EzyKafkaRequestInterceptor {
	
	void intercept(String cmd, Object requestData);
	
}
