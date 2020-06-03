package com.tvd12.ezymq.rabbitmq.testing.mockup;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class ConnectionFactoryMockup extends ConnectionFactory {

	@Override
	public Connection newConnection() throws IOException, TimeoutException {
		return new ConnectionMockup();
	}
	
}
