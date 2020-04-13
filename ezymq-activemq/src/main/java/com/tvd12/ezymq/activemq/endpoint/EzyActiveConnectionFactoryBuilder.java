package com.tvd12.ezymq.activemq.endpoint;

import javax.jms.ConnectionFactory;
import javax.jms.ExceptionListener;

import org.apache.activemq.ActiveMQConnectionFactory;

import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.io.EzyStrings;
import com.tvd12.ezymq.activemq.handler.EzyActiveExceptionListener;

public class EzyActiveConnectionFactoryBuilder implements EzyBuilder<ConnectionFactory> {
	protected String username;
	protected String password;
	protected String uri = null;
	protected int maxThreadPoolSize;
	protected ExceptionListener exceptionListener;
	
	public EzyActiveConnectionFactoryBuilder uri(String uri) {
		this.uri = uri;
		return this;
	}
	
	public EzyActiveConnectionFactoryBuilder username(String username) {
		this.username = username;
		return this;
	}
	
	public EzyActiveConnectionFactoryBuilder password(String password) {
		this.password = password;
		return this;
	}
	
	public EzyActiveConnectionFactoryBuilder maxThreadPoolSize(int maxThreadPoolSize) {
		this.maxThreadPoolSize = maxThreadPoolSize;
		return this;
	}
	
	public EzyActiveConnectionFactoryBuilder exceptionListener(ExceptionListener exceptionListener) {
		this.exceptionListener = exceptionListener;
		return this;
	}
	
	@Override
	public ConnectionFactory build() {
		if(exceptionListener == null)
			exceptionListener = newExceptionListner();
		ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
		if(!EzyStrings.isNoContent(uri))
			setConnectionURI(factory);
		if(!EzyStrings.isEmpty(username))
			factory.setUserName(username);
		if(!EzyStrings.isEmpty(password))
			factory.setPassword(password);
		if(maxThreadPoolSize > 0)
			factory.setMaxThreadPoolSize(maxThreadPoolSize);
		factory.setExceptionListener(exceptionListener);
		return factory;
	}
	
	private void setConnectionURI(ActiveMQConnectionFactory connectionFactory) {
		try {
			connectionFactory.setBrokerURL(uri);
		}
		catch(Exception e) {
			throw new IllegalArgumentException("uri: " + uri + " is invalid", e);
		}
	}
	
	protected ExceptionListener newExceptionListner() {
		return new EzyActiveExceptionListener();
	}
	
}
