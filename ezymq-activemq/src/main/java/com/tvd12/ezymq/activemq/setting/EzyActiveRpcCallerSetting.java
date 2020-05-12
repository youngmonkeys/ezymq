package com.tvd12.ezymq.activemq.setting;

import javax.jms.Destination;
import javax.jms.Session;

import lombok.Getter;

@Getter
public class EzyActiveRpcCallerSetting extends EzyActiveRpcEndpointSetting {

	protected final int capacity;
    protected final int defaultTimeout;
    
    public EzyActiveRpcCallerSetting(
    		Session session,
    		int capacity,
    		int defaultTimeout,
    		int threadPoolSize, 
    		String requestQueueName, 
    		String replyQueueName,
	        Destination requestQueue, 
	        Destination replyQueue) {
		super(
			session,
			threadPoolSize, 
			requestQueueName, 
			replyQueueName, 
			requestQueue, 
			replyQueue 
		);
		this.capacity = capacity;
		this.defaultTimeout = defaultTimeout;
	}
}
