package com.tvd12.ezymq.rabbitmq.endpoint;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AddressResolver;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.tvd12.ezyfox.util.EzyCloseable;

public class EzyRabbitConnectionFactory 
		extends ConnectionFactory implements EzyCloseable {

	protected ExecutorService copyExecutorService;
	protected final List<Connection> createdConnections
			= Collections.synchronizedList(new ArrayList<>());
	protected final Logger logger = LoggerFactory.getLogger(getClass());

	@Override
	public void setSharedExecutor(ExecutorService executor) {
		super.setSharedExecutor(executor);
		this.copyExecutorService = executor;
	}
	
	@Override
	public Connection newConnection(
			ExecutorService executor, 
			AddressResolver addressResolver, 
			String clientProvidedName) throws IOException, TimeoutException {
		Connection connection = super.newConnection(executor, addressResolver, clientProvidedName);
		createdConnections.add(connection);
		return connection;
	}
	
	@Override
	public void close() {
		for(Connection connection : createdConnections)
			closeConnection(connection);
		if(copyExecutorService != null)
			copyExecutorService.shutdown();
	}
	
	protected void closeConnection(Connection connection) {
		try {
			connection.close();
		}
		catch (Exception e) {
			logger.warn("close connection: {}, failed", e);
		}
	}

}
