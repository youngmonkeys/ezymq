package com.tvd12.ezymq.rabbitmq.endpoint;

import java.io.IOException;

import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import com.tvd12.ezyfox.io.EzyStrings;
import com.tvd12.ezyfox.util.EzyStartable;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitMessageHandler;

import lombok.Setter;

public class EzyRabbitTopicServer 
		extends EzyRabbitEndpoint implements EzyStartable  {

    protected final String queueName;
    protected CancelCallback cancelCallback;
    protected DeliverCallback deliverCallback;
    @Setter
    protected EzyRabbitMessageHandler messageHandler;
	
	public EzyRabbitTopicServer(
			Channel channel, 
			String exchange, 
			String queueName) throws IOException {
        super(channel, exchange);
        this.queueName = fetchQueueName(queueName);
    }
	
	protected String fetchQueueName(String queueName) throws IOException {
		if(EzyStrings.isNoContent(queueName))
			return channel.queueDeclare().getQueue();
		return queueName;
	}
	
	@Override
	public void start() throws Exception {
		this.cancelCallback = (consumerTag) -> {
        };
        this.deliverCallback = (consumerTag, delivery) -> {
        	messageHandler.handle(delivery);
        };
		channel.basicConsume(queueName, true, deliverCallback, cancelCallback);
	}
	
	public static Builder builder() {
		return new Builder();
	}
	
	public static class Builder extends EzyRabbitEndpoint.Builder<Builder> {

		protected String queueName; 
		
		public Builder queueName(String queueName) {
			this.queueName = queueName;
			return this;
		}
		
		@Override
		public EzyRabbitTopicServer build() {
			try {
				return new EzyRabbitTopicServer(
						channel, 
						exchange, 
						queueName);
			}
			catch(Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
	
}
