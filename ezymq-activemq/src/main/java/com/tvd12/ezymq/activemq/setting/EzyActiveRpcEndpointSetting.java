package com.tvd12.ezymq.activemq.setting;

import javax.jms.Destination;
import javax.jms.Session;

import lombok.Getter;

@Getter
public class EzyActiveRpcEndpointSetting extends EzyActiveEndpointSetting {

	protected int threadPoolSize;
    protected String requestQueueName;
	protected String replyQueueName;
    protected Destination requestQueue;
	protected Destination replyQueue;
	
	public EzyActiveRpcEndpointSetting(
			Session session,
			int threadPoolSize,
			String requestQueueName,
			String replyQueueName,
			Destination requestQueue,
			Destination replyQueue) {
		super(session);
		this.threadPoolSize = threadPoolSize;
		this.requestQueue = requestQueue;
		this.replyQueue = replyQueue;
		this.requestQueueName = requestQueueName;
		this.replyQueueName = replyQueueName;
	}
	
}
