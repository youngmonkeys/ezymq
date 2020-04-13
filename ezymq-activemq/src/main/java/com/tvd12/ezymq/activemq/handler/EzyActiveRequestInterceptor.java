package com.tvd12.ezymq.activemq.handler;

public interface EzyActiveRequestInterceptor {
	
	void intercept(String cmd, Object requestData);
	
}
