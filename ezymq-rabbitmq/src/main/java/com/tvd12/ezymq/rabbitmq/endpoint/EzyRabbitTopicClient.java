package com.tvd12.ezymq.rabbitmq.endpoint;

import java.io.IOException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;

public class EzyRabbitTopicClient extends EzyRabbitEndpoint {

    protected final String routingKey;
	
	public EzyRabbitTopicClient(
			Channel channel, String exchange, String routingKey) {
        super(channel, exchange);
        this.routingKey = routingKey;
    }
	
	public void publish(AMQP.BasicProperties props, byte[] message) 
			throws IOException {
		channel.basicPublish(exchange, routingKey, props, message);
	}
	
	public static Builder builder() {
		return new Builder();
	}
	
	public static class Builder extends EzyRabbitEndpoint.Builder<Builder> {

		protected String routingKey; 
		
		public Builder routingKey(String routingKey) {
			this.routingKey = routingKey;
			return this;
		}
		
		@Override
		public EzyRabbitTopicClient build() {
			return new EzyRabbitTopicClient(
					channel, 
					exchange, 
					routingKey);
		}
	}
	
}
