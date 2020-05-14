package com.tvd12.ezymq.activemq.manager;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Session;

import com.tvd12.ezyfox.util.EzyLoggable;
import com.tvd12.ezymq.activemq.setting.EzyActiveEndpointSetting;

public class EzyActiveAbstractManager extends EzyLoggable {

	protected final ConnectionFactory connectionFactory;
	
	public EzyActiveAbstractManager(ConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}
	
	public Session getSession(EzyActiveEndpointSetting setting) throws Exception {
		Session session = setting.getSession();
		if(session == null) {
			Connection connection = connectionFactory.createConnection();
			connection.start();
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		}
		return session;
	}
	
}
