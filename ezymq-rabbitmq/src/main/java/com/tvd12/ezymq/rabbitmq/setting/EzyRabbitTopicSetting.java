package com.tvd12.ezymq.rabbitmq.setting;

import com.rabbitmq.client.Channel;

import lombok.Getter;

@Getter
public class EzyRabbitTopicSetting extends EzyRabbitEndpointSetting {

	protected final boolean clientEnable;
	protected final String clientRoutingKey;
	protected final boolean serverEnable;
	protected final String serverQueueName;
	
	public EzyRabbitTopicSetting(
			Channel channel, 
			String exchange,
			int prefetchCount,
			boolean clientEnable,
			String clientRoutingKey,
			boolean serverEnable,
			String serverQueueName) {
		super(channel, exchange, prefetchCount);
		this.clientEnable = clientEnable;
		this.clientRoutingKey = clientRoutingKey;
		this.serverEnable = serverEnable;
		this.serverQueueName = serverQueueName;
	}
	
	public static Builder builder() {
		return new Builder();
	}
	
	public static class Builder extends EzyRabbitEndpointSetting.Builder<Builder> {

		protected boolean clientEnable;
		protected String clientRoutingKey;
		protected boolean serverEnable;
		protected String serverQueueName;
		protected EzyRabbitSettings.Builder parent;
		
		public Builder() {
			this(null);
		}
		
		public Builder(EzyRabbitSettings.Builder parent) {
			this.parent = parent;
		}
		
		public Builder clientEnable(boolean clientEnable) {
			this.clientEnable = clientEnable;
			return this;
		}
		
		public Builder clientRoutingKey(String clientRoutingKey) {
			this.clientRoutingKey = clientRoutingKey;
			return this;
		}
		
		public Builder serverEnable(boolean serverEnable) {
			this.serverEnable = serverEnable;
			return this;
		}
		
		public Builder serverQueueName(String serverQueueName) {
			this.serverQueueName = serverQueueName;
			return this;
		}
		
		public EzyRabbitSettings.Builder parent() {
			return parent;
		}
		
		@Override
		public EzyRabbitTopicSetting build() {
			return new EzyRabbitTopicSetting(
					channel,
					exchange,
					prefetchCount,
					clientEnable,
					clientRoutingKey,
					serverEnable,
					serverQueueName
			);
		}
	}

}
