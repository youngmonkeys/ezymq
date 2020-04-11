package com.tvd12.ezymq.rabbitmq.handler;

import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.AMQP.BasicProperties;

public interface EzyRabbitMessageHandler {
	
	void handle(
			BasicProperties requestProperties,
			byte[] requestBody);
	
	default void handle(Delivery request) {
		handle( request.getProperties(),
				request.getBody());
	}
	
}
