package com.tvd12.ezymq.activemq.endpoint;

import java.util.concurrent.ThreadFactory;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import com.tvd12.ezyfox.concurrent.EzyThreadList;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezyfox.util.EzyProcessor;
import com.tvd12.ezymq.activemq.concurrent.EzyActiveThreadFactory;
import com.tvd12.ezymq.activemq.constant.EzyActiveDestinationType;
import com.tvd12.ezymq.activemq.util.EzyActiveProperties;

public abstract class EzyActiveRpcEndpoint 
		extends EzyActiveEndpoint implements EzyCloseable {

	protected volatile boolean active;
    protected final int threadPoolSize;
    protected final Destination requestQueue;
	protected final Destination replyQueue;
	protected final MessageProducer producer;
	protected final MessageConsumer consumer;
	protected final EzyThreadList executorService;
	
	public EzyActiveRpcEndpoint(
			Session session, 
			Destination requestQueue, 
			Destination replyQueue,
			int threadPoolSize) throws Exception {
		super(session);
		this.requestQueue = requestQueue;
		this.replyQueue = replyQueue;
		this.threadPoolSize = threadPoolSize;
        this.producer = session.createProducer(requestQueue);
        this.consumer = session.createConsumer(replyQueue);
        this.executorService = newExecutorSerivice();
    }
	
	protected EzyThreadList newExecutorSerivice() {
		ThreadFactory threadFactory 
			= EzyActiveThreadFactory.create("rpc-client");
		EzyThreadList executorService = 
				new EzyThreadList(threadPoolSize, () -> loop(), threadFactory);
		return executorService;
	}
	
	protected final void loop() {
		while(active) {
			handleLoopOne();
		}
	}
	
	protected abstract void handleLoopOne();

	protected void publish(EzyActiveProperties props, byte[] message) 
			throws Exception {
		BytesMessage m = session.createBytesMessage();
		setMessageProperties(m, props);
		m.writeBytes(message);
		producer.send(m);
	}
	
	public void close() {
		this.active = false;
		EzyProcessor.processWithLogException(() -> producer.close());
		EzyProcessor.processWithLogException(() -> consumer.close());
    }
	
	@SuppressWarnings("unchecked")
	public abstract static class Builder<B extends Builder<B>> 
			extends EzyActiveEndpoint.Builder<B> {
		
	    protected int threadPoolSize;
	    protected String requestQueueName;
		protected String replyQueueName;
	    protected Destination requestQueue;
		protected Destination replyQueue;
		
		public B threadPoolSize(int threadPoolSize) {
			this.threadPoolSize = threadPoolSize ;
			return (B)this;
		}
		
		public B requestQueue(Destination requestQueue) {
			this.requestQueue = requestQueue;
			return (B)this;
		}
		
		public B replyQueue(Destination replyQueue) {
			this.replyQueue = replyQueue;
			return (B)this;
		}
		
		public B requestQueueName(String requestQueueName) {
			this.requestQueueName = requestQueueName;
			return (B)this;
		}
		
		public B replyQueueName(String replyQueueName) {
			this.replyQueueName = replyQueueName;
			return (B)this;
		}
		
		@Override
		public EzyActiveRpcEndpoint build() {
			if(replyQueue == null)
				replyQueue = createDestination(EzyActiveDestinationType.QUEUE, requestQueueName);
			if(replyQueue == null)
				replyQueue = createDestination(EzyActiveDestinationType.QUEUE, replyQueueName);
			try {
				return newProduct();
			}
			catch(Exception e) {
				throw new RuntimeException(e);
			}
		}
		
		protected abstract EzyActiveRpcEndpoint newProduct() throws Exception;
		
	}
	
}
