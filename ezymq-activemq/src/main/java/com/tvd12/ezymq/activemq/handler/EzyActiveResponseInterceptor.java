package com.tvd12.ezymq.activemq.handler;

public interface EzyActiveResponseInterceptor {
	
	void intercept(String cmd, Object requestData, Object responseData);
	
}
