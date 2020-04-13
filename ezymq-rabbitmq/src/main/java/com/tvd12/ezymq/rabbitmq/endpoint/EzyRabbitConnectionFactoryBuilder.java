package com.tvd12.ezymq.rabbitmq.endpoint;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ExceptionHandler;
import com.rabbitmq.client.impl.ForgivingExceptionHandler;
import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.io.EzyStrings;
import com.tvd12.ezymq.rabbitmq.concurrent.EzyRabbitThreadFactory;

public class EzyRabbitConnectionFactoryBuilder implements EzyBuilder<ConnectionFactory> {
	protected int port = 5672;
	protected String username = "guest";
	protected String password = "guest";
	protected String host = "localhost";
	protected String vhost = "/";
	protected String uri = null;
	protected int sharedThreadPoolSize;
	protected ThreadFactory threadFactory;
	protected ExceptionHandler exceptionHandler;
	
	public EzyRabbitConnectionFactoryBuilder uri(String uri) {
		this.uri = uri;
		return this;
	}
	
	public EzyRabbitConnectionFactoryBuilder host(String host) {
		this.host = host;
		return this;
	}
	
	public EzyRabbitConnectionFactoryBuilder vhost(String vhost) {
		this.vhost = vhost;
		return this;
	}
	
	public EzyRabbitConnectionFactoryBuilder port(int port) {
		this.port = port;
		return this;
	}
	
	public EzyRabbitConnectionFactoryBuilder username(String username) {
		this.username = username;
		return this;
	}
	
	public EzyRabbitConnectionFactoryBuilder password(String password) {
		this.password = password;
		return this;
	}
	
	public EzyRabbitConnectionFactoryBuilder threadFactory(ThreadFactory factory) {
		this.threadFactory = factory;
		return this;
	}
	
	public EzyRabbitConnectionFactoryBuilder threadFactory(String poolName) {
		return threadFactory(EzyRabbitThreadFactory.create(poolName));
	}
	
	public EzyRabbitConnectionFactoryBuilder sharedThreadPoolSize(int sharedThreadPoolSize) {
		this.sharedThreadPoolSize = sharedThreadPoolSize;
		return this;
	}
	
	public EzyRabbitConnectionFactoryBuilder exceptionHandler(ExceptionHandler exceptionHandler) {
		this.exceptionHandler = exceptionHandler;
		return this;
	}
	
	@Override
	public ConnectionFactory build() {
		if(threadFactory == null)
			threadFactory = newThreadFactory();
		if(exceptionHandler == null)
			exceptionHandler = newExceptionHandler();
		ConnectionFactory factory = new ConnectionFactory();
		if(EzyStrings.isNoContent(uri)) {
			factory.setHost(host);
			factory.setPort(port);
			factory.setUsername(username);
			factory.setPassword(password);
			factory.setVirtualHost(vhost);
		}
		else {
			setConnectionURI(factory);
		}
		factory.setThreadFactory(threadFactory);
		factory.setExceptionHandler(exceptionHandler);
		if(sharedThreadPoolSize > 0)
			factory.setSharedExecutor(Executors.newFixedThreadPool(sharedThreadPoolSize, threadFactory));
		return factory;
	}
	
	private void setConnectionURI(ConnectionFactory connectionFactory) {
		try {
			connectionFactory.setUri(uri);
		}
		catch(Exception e) {
			throw new IllegalArgumentException("uri: " + uri + " is invalid", e);
		}
	}
	
	private ThreadFactory newThreadFactory() {
		return EzyRabbitThreadFactory.create("worker");
	}
	
	protected ExceptionHandler newExceptionHandler() {
		return new ForgivingExceptionHandler();
	}
	
}
