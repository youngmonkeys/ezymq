package com.tvd12.ezymq.rabbitmq.endpoint;

import java.io.IOException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import com.tvd12.ezyfox.io.EzyStrings;
import com.tvd12.ezyfox.util.EzyStartable;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitMessageHandler;

import lombok.Setter;

public class EzyRabbitTopicServer 
		extends EzyRabbitEndpoint implements EzyStartable  {

    protected final String queueName;
    protected final Consumer consumer;
    @Setter
    protected EzyRabbitMessageHandler messageHandler;
	
	public EzyRabbitTopicServer(
			Channel channel, 
			String exchange, 
			String queueName) throws IOException {
        super(channel, exchange);
        this.queueName = fetchQueueName(queueName);
        this.consumer = newConsumer();
    }
	
	protected String fetchQueueName(String queueName) throws IOException {
		if(EzyStrings.isNoContent(queueName))
			return channel.queueDeclare().getQueue();
		return queueName;
	}
	
	@Override
	public void start() throws Exception {
        channel.basicConsume(queueName, true, consumer);
	}
	
	protected Consumer newConsumer() {
		return new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(
					String consumerTag, 
					Envelope envelope, 
					BasicProperties properties, 
					byte[] body) throws IOException {
				Delivery delivery = new Delivery(envelope, properties, body);
				messageHandler.handle(delivery);
			}
		};
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
