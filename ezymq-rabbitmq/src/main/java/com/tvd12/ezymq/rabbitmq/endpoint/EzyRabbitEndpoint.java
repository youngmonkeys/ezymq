package com.tvd12.ezymq.rabbitmq.endpoint;

import com.rabbitmq.client.Channel;
import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.util.EzyLoggable;

public class EzyRabbitEndpoint extends EzyLoggable {

    protected final Channel channel;
    protected final String exchange;
	
	public EzyRabbitEndpoint(Channel channel, String exchange) {
        this.channel = channel;
        this.exchange = exchange;
    }
	
	@SuppressWarnings("unchecked")
	public static abstract class Builder<B extends Builder<B>> implements EzyBuilder<EzyRabbitEndpoint> {
		
		protected Channel channel; 
		protected String exchange; 
		
		public B channel(Channel channel) {
			this.channel = channel;
			return (B)this;
		}
		
		public B exchange(String exchange) {
			this.exchange = exchange;
			return (B)this;
		}

	}
	
}
