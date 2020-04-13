package com.tvd12.ezymq.activemq.handler;

import com.tvd12.ezymq.activemq.util.EzyActiveProperties;

public interface EzyActiveRpcCallHandler {
	
	void handleFire(
			EzyActiveProperties requestProperties,
			byte[] requestBody);
	
	byte[] handleCall(
			EzyActiveProperties requestProperties,
			byte[] requestBody, 
			EzyActiveProperties.Builder replyPropertiesBuilder);
	
}
