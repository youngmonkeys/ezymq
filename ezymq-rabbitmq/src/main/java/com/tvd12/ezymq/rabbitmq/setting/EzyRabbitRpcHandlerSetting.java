package com.tvd12.ezymq.rabbitmq.setting;

import java.util.Map;

import com.rabbitmq.client.Channel;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitActionInterceptor;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitRequestHandler;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitRequestHandlers;

import lombok.Getter;

@Getter
public class EzyRabbitRpcHandlerSetting extends EzyRabbitEndpointSetting {

	protected final int threadPoolSize;
	protected final String replyRoutingKey;
	protected final String requestQueueName;
	protected final EzyRabbitRequestHandlers requestHandlers;
	protected final EzyRabbitActionInterceptor actionInterceptor;
	
	public EzyRabbitRpcHandlerSetting(
			Channel channel, 
			String exchange, 
			String replyRoutingKey,
			String requestQueueName,
			int threadPoolSize,
			EzyRabbitRequestHandlers requestHandlers,
			EzyRabbitActionInterceptor actionInterceptor) {
		super(channel, exchange);
		this.replyRoutingKey = replyRoutingKey;
		this.requestQueueName = requestQueueName;
		this.threadPoolSize = threadPoolSize;
		this.requestHandlers = requestHandlers;
		this.actionInterceptor = actionInterceptor;
	}
	
	public static Builder builder() {
		return new Builder();
	}
	
	public static class Builder extends EzyRabbitEndpointSetting.Builder<Builder> {

		protected int threadPoolSize = 3;
		protected String replyRoutingKey = "";
		protected String requestQueueName = null;
		protected EzyRabbitRequestHandlers requestHandlers;
		protected EzyRabbitActionInterceptor actionInterceptor;
		protected EzyRabbitSettings.Builder parent;
		
		public Builder() {
			this(null);
		}
		
		public Builder(EzyRabbitSettings.Builder parent) {
			this.parent = parent;
		}
		
		public Builder channel(Channel channel) {
			this.channel = channel;
			return this;
		}
		
		public Builder exchange(String exchange) {
			this.exchange = exchange;
			return this;
		}
		
		public Builder threadPoolSize(int threadPoolSize) {
			this.threadPoolSize = threadPoolSize;
			return this;
		}
		
		public Builder requestQueueName(String requestQueueName) {
			this.requestQueueName = requestQueueName;
			return this;
		}
		
		public Builder replyRoutingKey(String replyRoutingKey) {
			this.replyRoutingKey = replyRoutingKey;
			return this;
		}
		
		public Builder requestHandlers(EzyRabbitRequestHandlers requestHandlers) {
			this.requestHandlers = requestHandlers;
			return this;
		}
		
		public Builder actionInterceptor(EzyRabbitActionInterceptor actionInterceptor) {
			this.actionInterceptor = actionInterceptor;
			return this;
		}
		
		@SuppressWarnings("rawtypes")
		public Builder addRequestHandler(String cmd, EzyRabbitRequestHandler handler) {
			if(requestHandlers == null)
				requestHandlers = new EzyRabbitRequestHandlers();
			requestHandlers.addHandler(cmd, handler);
			return this;
	    }
		
		@SuppressWarnings("rawtypes")
		public Builder addRequestHandler(Map<String, EzyRabbitRequestHandler> handlers) {
			for(String cmd : handlers.keySet()) {
				EzyRabbitRequestHandler handler = handlers.get(cmd);
				addRequestHandler(cmd, handler);
			}
			return this;
	    }
		
		public EzyRabbitSettings.Builder parent() {
			return parent;
		}
		
		@Override
		public EzyRabbitRpcHandlerSetting build() {
			if(requestHandlers == null)
				throw new NullPointerException("requestHandlers can not be null");
			return new EzyRabbitRpcHandlerSetting(
					channel, 
					exchange,
					replyRoutingKey, 
					requestQueueName,
					threadPoolSize,
					requestHandlers,
					actionInterceptor);
		}
		
	}

}
