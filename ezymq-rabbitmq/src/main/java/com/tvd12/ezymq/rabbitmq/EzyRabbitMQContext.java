package com.tvd12.ezymq.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;
import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezymq.rabbitmq.codec.EzyRabbitDataCodec;
import com.tvd12.ezymq.rabbitmq.manager.EzyRabbitRpcCallerManager;
import com.tvd12.ezymq.rabbitmq.manager.EzyRabbitRpcHandlerManager;
import com.tvd12.ezymq.rabbitmq.manager.EzyRabbitTopicManager;
import com.tvd12.ezymq.rabbitmq.setting.EzyRabbitSettings;

public class EzyRabbitMQContext {

	protected final EzyRabbitSettings settings;
	protected final EzyEntityCodec entityCodec;
	protected final EzyRabbitDataCodec dataCodec;
	protected final EzyRabbitTopicManager topicManager;
	protected final ConnectionFactory connectionFactory;
	protected final EzyRabbitRpcCallerManager rpcCallerManager;
	protected final EzyRabbitRpcHandlerManager rpcHandlerManager;
	
	public EzyRabbitMQContext(
			EzyEntityCodec entityCodec,
			EzyRabbitDataCodec dataCodec,
			EzyRabbitSettings settings,
			ConnectionFactory connectionFactory) {
		this.settings = settings;
		this.dataCodec = dataCodec;
		this.entityCodec = entityCodec;
		this.connectionFactory = connectionFactory;
		this.topicManager = newTopicManager();
		this.rpcCallerManager = newRpcCallerManager();
		this.rpcHandlerManager = newRabbitRpcHandlerManager();
		
	}
	
	public <T> EzyRabbitTopic<T> getTopic(String name) {
		return topicManager.getTopic(name);
	}
	
	public EzyRabbitRpcCaller getRpcCaller(String name) {
		return rpcCallerManager.getRpcCaller(name);
	}
	
	public EzyRabbitRpcHandler getRabbitRpcHandler(String name) {
		return rpcHandlerManager.getRpcHandler(name);
	}
	
	protected EzyRabbitTopicManager newTopicManager() {
		return new EzyRabbitTopicManager(
				dataCodec,
				connectionFactory,
				settings.getTopicSettings()
		);
	}
	
	protected EzyRabbitRpcCallerManager newRpcCallerManager() {
		return new EzyRabbitRpcCallerManager(
				entityCodec,
				connectionFactory,
				settings.getRpcCallerSettings()
		);
	}
	
	protected EzyRabbitRpcHandlerManager newRabbitRpcHandlerManager() {
		return new EzyRabbitRpcHandlerManager(
				dataCodec,
				connectionFactory,
				settings.getRpcHandlerSettings()
		);
	}
	
	public static EzyRabbitMQContextBuilder builder() {
		return new EzyRabbitMQContextBuilder();
	}
	
}
