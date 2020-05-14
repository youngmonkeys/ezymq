package com.tvd12.ezymq.activemq.setting;

import javax.jms.Destination;
import javax.jms.Session;

import com.tvd12.ezymq.activemq.factory.EzyActiveCorrelationIdFactory;
import com.tvd12.ezymq.activemq.handler.EzyActiveResponseConsumer;

import lombok.Getter;

@Getter
public class EzyActiveRpcCallerSetting extends EzyActiveRpcEndpointSetting {

	protected final int capacity;
    protected final int defaultTimeout;
    protected final EzyActiveCorrelationIdFactory correlationIdFactory;
	protected final EzyActiveResponseConsumer unconsumedResponseConsumer;
    
    public EzyActiveRpcCallerSetting(
    		Session session,
    		String requestQueueName, 
	        Destination requestQueue,
	        String replyQueueName,
	        Destination replyQueue,
	        int capacity,
	        int threadPoolSize, 
	        int defaultTimeout,
	        EzyActiveCorrelationIdFactory correlationIdFactory,
	        EzyActiveResponseConsumer unconsumedResponseConsumer) {
		super(
			session,
			requestQueueName, 
			requestQueue,
			replyQueueName,
			replyQueue,
			threadPoolSize
		);
		this.capacity = capacity;
		this.defaultTimeout = defaultTimeout;
		this.correlationIdFactory = correlationIdFactory;
		this.unconsumedResponseConsumer = unconsumedResponseConsumer;
	}
    
    public static Builder builder() {
		return new Builder();
	}
	
	public static class Builder extends EzyActiveRpcEndpointSetting.Builder<Builder> {
		
		protected int capacity;
	    protected int defaultTimeout;
		protected EzyActiveCorrelationIdFactory correlationIdFactory;
		protected EzyActiveResponseConsumer unconsumedResponseConsumer;
		protected EzyActiveSettings.Builder parent;
		
		public Builder() {
			this(null);
		}
		
		public Builder(EzyActiveSettings.Builder parent) {
			this.capacity = 10000;
			this.parent = parent;
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
		
		public EzyActiveSettings.Builder parent() {
			return parent;
		}
		
		@Override
		public EzyActiveRpcCallerSetting build() {
			return new EzyActiveRpcCallerSetting(
					session, 
					requestQueueName,
					requestQueue, 
					replyQueueName,
					replyQueue, 
					capacity, 
					threadPoolSize,
					defaultTimeout,
					correlationIdFactory,
					unconsumedResponseConsumer);
		}
		
	}
}
