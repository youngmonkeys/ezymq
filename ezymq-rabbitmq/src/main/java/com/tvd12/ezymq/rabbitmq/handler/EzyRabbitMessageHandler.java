package com.tvd12.ezymq.rabbitmq.handler;

import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.AMQP.BasicProperties;

public interface EzyRabbitMessageHandler {
	
	void handle(
			BasicProperties properties,
			byte[] messageBody);
	
	default void handle(Delivery request) {
		handle( request.getProperties(),
				request.getBody());
	}
	
}
