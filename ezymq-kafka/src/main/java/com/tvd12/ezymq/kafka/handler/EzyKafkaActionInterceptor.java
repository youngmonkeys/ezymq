package com.tvd12.ezymq.kafka.handler;

public interface EzyKafkaActionInterceptor extends
		EzyKafkaRequestInterceptor, 
		EzyKafkaResponseInterceptor, 
		EzyKafkaExceptionInterceptor{

}
