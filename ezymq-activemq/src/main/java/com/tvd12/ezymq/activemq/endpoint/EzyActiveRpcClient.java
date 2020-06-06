package com.tvd12.ezymq.activemq.endpoint;

import java.util.concurrent.TimeoutException;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import com.tvd12.ezyfox.concurrent.EzyFuture;
import com.tvd12.ezyfox.concurrent.EzyFutureConcurrentHashMap;
import com.tvd12.ezyfox.concurrent.EzyFutureMap;
import com.tvd12.ezymq.activemq.exception.EzyActiveMaxCapacity;
import com.tvd12.ezymq.activemq.factory.EzyActiveCorrelationIdFactory;
import com.tvd12.ezymq.activemq.factory.EzyActiveSimpleCorrelationIdFactory;
import com.tvd12.ezymq.activemq.handler.EzyActiveResponseConsumer;
import com.tvd12.ezymq.activemq.util.EzyActiveProperties;

public class EzyActiveRpcClient extends EzyActiveRpcEndpoint {

	protected final int capacity;
    protected final int defaultTimeout;
	protected final EzyFutureMap<String> futureMap;
	protected final EzyActiveCorrelationIdFactory correlationIdFactory;
	protected final EzyActiveResponseConsumer unconsumedResponseConsumer;
	
	protected final static int NO_TIMEOUT = -1;
	
	public EzyActiveRpcClient(
			Session session, 
			Destination requestQueue, 
			Destination replyQueue,
			int capacity,
			int threadPoolSize,
			int defaultTimeout, 
			EzyActiveResponseConsumer unconsumedResponseConsumer) throws Exception {
        this(session,
        		requestQueue,
        		replyQueue,
        		capacity,
        		threadPoolSize,
        		defaultTimeout,
        		new EzyActiveSimpleCorrelationIdFactory(),
        		unconsumedResponseConsumer);
    }
	
	public EzyActiveRpcClient(
			Session session, 
			Destination requestQueue, 
			Destination replyQueue,
			int capacity,
			int threadPoolSize,
			int defaultTimeout, 
			EzyActiveCorrelationIdFactory correlationIdFactory,
			EzyActiveResponseConsumer unconsumedResponseConsumer) throws Exception {
		super(session, requestQueue, replyQueue, threadPoolSize);
		this.capacity = capacity;
        this.defaultTimeout = defaultTimeout;
        this.correlationIdFactory = correlationIdFactory;
        this.futureMap = new EzyFutureConcurrentHashMap<>();
        this.unconsumedResponseConsumer = unconsumedResponseConsumer;
        this.active = true;
        this.executorService.execute();
    }
	
	@Override
	protected MessageProducer createProducer() throws Exception {
		return session.createProducer(requestQueue);
	}
	
	@Override
	protected MessageConsumer createConsumer() throws Exception {
		return session.createConsumer(replyQueue);
	}
	
	@Override
	protected void handleLoopOne() {
		String replyId = null;
		EzyFuture future = null;
		byte[] responseBody = null;
		Exception exception = null;
		EzyActiveProperties properties = null;
		try {
			BytesMessage message = (BytesMessage) consumer.receive();
			if(message == null)
				return;
			replyId = message.getJMSCorrelationID();
			future = futureMap.removeFuture(replyId);
			properties = getMessageProperties(message);
			responseBody = getMessageBody(message);
		}
		catch (Exception e) {
			exception = e;
		}
		if(future != null) {
			if(exception != null)
				future.setException(exception);
			else
				future.setResult(new EzyActiveMessage(properties, responseBody));
		}
		else {
			if(unconsumedResponseConsumer != null) {
    			unconsumedResponseConsumer.consume(properties, responseBody);
    		}
    		else { 
    			logger.warn("No outstanding request for correlation ID {}", replyId);
    		}
		}
	}
	
	public void doFire(EzyActiveProperties props, byte[] message)
			throws Exception {
        EzyActiveProperties newProperties = props != null 
        		? props 
        		: EzyActiveProperties.builder().build();
        publish(newProperties, message);
	}
	
	public EzyActiveMessage doCall(EzyActiveProperties props, byte[] message)
	        throws Exception {
		return doCall(props, message, defaultTimeout);
	}
 
	public EzyActiveMessage doCall(EzyActiveProperties props, byte[] message, int timeout)
	        throws Exception {
		if(futureMap.size() >= capacity)
			throw new EzyActiveMaxCapacity("rpc client too many request, capacity: " + capacity);
        String replyId = correlationIdFactory.newCorrelationId();
    	EzyActiveProperties newProperties = EzyActiveProperties.builder()
    			.addProperties(props)
    			.correlationId(replyId)
    			.build();
    	EzyFuture future = futureMap.addFuture(replyId);
        publish(newProperties, message);
        try {
            return future.get(timeout);
        } catch (TimeoutException ex) {
        	futureMap.removeFuture(replyId);
            throw ex;
        }
        
	}
	
	@Override
	protected String getThreadName() {
		return "rpc-client";
	}
	
	public static Builder builder() {
		return new Builder();
	}
	
	public static class Builder extends EzyActiveRpcEndpoint.Builder<Builder> {
		
		protected int capacity;
	    protected int defaultTimeout;
		protected EzyActiveCorrelationIdFactory correlationIdFactory;
		protected EzyActiveResponseConsumer unconsumedResponseConsumer;
		
		public Builder() {
			this.capacity = 10000;
		}
		
		public Builder capacity(int capacity) {
			this.capacity = capacity;
			return this;
		}
		
		public Builder defaultTimeout(int defaultTimeout) {
			this.defaultTimeout = defaultTimeout;
			return this;
		}
		
		public Builder correlationIdFactory(EzyActiveCorrelationIdFactory correlationIdFactory) {
			this.correlationIdFactory = correlationIdFactory;
			return this;
		}
		
		public Builder unconsumedResponseConsumer(EzyActiveResponseConsumer unconsumedResponseConsumer) {
			this.unconsumedResponseConsumer = unconsumedResponseConsumer;
			return this;
		}
		
		@Override
		public EzyActiveRpcClient build() {
			if(correlationIdFactory == null)
				correlationIdFactory = new EzyActiveSimpleCorrelationIdFactory();
			return (EzyActiveRpcClient)super.build();
		}
		
		@Override
		protected EzyActiveRpcEndpoint newProduct() throws Exception {
			return new EzyActiveRpcClient(
					session, 
					requestQueue, 
					replyQueue, 
					capacity, 
					threadPoolSize,
					defaultTimeout, 
					correlationIdFactory,
					unconsumedResponseConsumer);
		}
		
	}
	
}
