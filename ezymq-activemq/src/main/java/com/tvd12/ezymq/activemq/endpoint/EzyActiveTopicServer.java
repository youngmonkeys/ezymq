package com.tvd12.ezymq.activemq.endpoint;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import com.tvd12.ezyfox.util.EzyStartable;
import com.tvd12.ezyfox.util.EzyStoppable;
import com.tvd12.ezymq.activemq.concurrent.EzyActiveThreadFactory;
import com.tvd12.ezymq.activemq.handler.EzyActiveMessageHandler;
import com.tvd12.ezymq.activemq.util.EzyActiveProperties;

import lombok.Setter;

public class EzyActiveTopicServer 
		extends EzyActiveTopicEndpoint 
		implements EzyStartable, EzyStoppable  {

	protected volatile boolean active;
	protected final int threadPoolSize;
	protected final MessageConsumer consumer;
	protected ExecutorService executorService;
    @Setter
    protected EzyActiveMessageHandler messageHandler;
	
    public EzyActiveTopicServer(
    		Session session, 
    		Destination topic,
    		int threadPoolSize) throws Exception {
		super(session, topic);
		this.threadPoolSize = threadPoolSize;
		this.consumer = session.createConsumer(topic);
	}
	
	@Override
	public void start() throws Exception {
		this.active = true;
		this.executorService = newExecutorSerivice();
		for(int i = 0 ; i < threadPoolSize ; ++i)
			executorService.execute(() -> loop());
	}
	
	protected void loop() {
		while(active) {
			BytesMessage message = null;
			try {
				message = (BytesMessage)consumer.receive();
				EzyActiveProperties props = getMessageProperties(message);
				byte[] body = getMessageBody(message);
				messageHandler.handle(props, body);
			} 
			catch (JMSException e) {
				logger.warn("receive topic message error", e);
			}
			catch(Exception e) {
				logger.warn("process message: {} error", message, e);
			}
		}
	}
	
	protected ExecutorService newExecutorSerivice() {
		ThreadFactory threadFactory 
			= EzyActiveThreadFactory.create("topic-server");
		ExecutorService executorService 
			= Executors.newFixedThreadPool(threadPoolSize, threadFactory);
		Runtime.getRuntime()
			.addShutdownHook(new Thread(() -> executorService.shutdown()));
		return executorService;
	}
	
	@Override
	public void stop() {
		this.active = false;
		if(executorService != null)
			executorService.shutdown();
		this.executorService = null;
	}
	
	public static Builder builder() {
		return new Builder();
	}
	
	public static class Builder extends EzyActiveTopicEndpoint.Builder<Builder> {

		protected int threadPoolSize = 3;
		
		public Builder threadPoolSize(int threadPoolSize) {
			this.threadPoolSize = threadPoolSize;
			return this;
		}
		
		@Override
		public EzyActiveTopicServer build() {
			return (EzyActiveTopicServer)super.build();
		}
		
		@Override
		protected EzyActiveTopicEndpoint newEnpoint() throws Exception {
			return new EzyActiveTopicServer(session, topic, threadPoolSize);
		}
	}
	
}
