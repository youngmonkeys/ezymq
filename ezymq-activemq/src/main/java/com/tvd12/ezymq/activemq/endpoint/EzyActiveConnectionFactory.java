package com.tvd12.ezymq.activemq.endpoint;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.jms.Connection;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.management.JMSStatsImpl;
import org.apache.activemq.transport.Transport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tvd12.ezyfox.util.EzyCloseable;

public class EzyActiveConnectionFactory 
		extends ActiveMQConnectionFactory
		implements EzyCloseable {
	
	protected final List<Connection> createdConnections
			= Collections.synchronizedList(new ArrayList<>());
	protected final Logger logger = LoggerFactory.getLogger(getClass());
	
	@Override
	protected ActiveMQConnection createActiveMQConnection(
			Transport transport, 
			JMSStatsImpl stats) throws Exception {
		ActiveMQConnection connection = super.createActiveMQConnection(transport, stats);
		createdConnections.add(connection);
		return connection;
	}
	
	@Override
	public void close() {
		for(Connection connection : createdConnections)
			closeConnection(connection);
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
