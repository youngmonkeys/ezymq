package com.tvd12.ezymq.activemq.setting;

import javax.jms.Destination;
import javax.jms.Session;

import com.tvd12.ezymq.activemq.handler.EzyActiveActionInterceptor;
import com.tvd12.ezymq.activemq.handler.EzyActiveRequestHandlers;

import lombok.Getter;

@Getter
public class EzyActiveRpcHandlerSetting extends EzyActiveRpcEndpointSetting {

	protected final EzyActiveRequestHandlers requestHandlers;
	protected final EzyActiveActionInterceptor actionInterceptor;
	
	public EzyActiveRpcHandlerSetting(
			Session session, 
			int threadPoolSize, 
			String requestQueueName,
	        String replyQueueName,
	        Destination requestQueue, 
	        Destination replyQueue,
	        EzyActiveRequestHandlers requestHandlers,
	        EzyActiveActionInterceptor actionInterceptor) {
		super(
			session, 
			threadPoolSize, 
			requestQueueName, 
			replyQueueName, 
			requestQueue, 
			replyQueue
		);
		this.requestHandlers = requestHandlers;
		this.actionInterceptor = actionInterceptor;
	}

}
