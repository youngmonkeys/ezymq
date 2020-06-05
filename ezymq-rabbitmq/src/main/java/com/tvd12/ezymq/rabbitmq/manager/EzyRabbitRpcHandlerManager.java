package com.tvd12.ezymq.rabbitmq.manager;

import java.util.HashMap;
import java.util.Map;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.tvd12.ezymq.rabbitmq.EzyRabbitRpcHandler;
import com.tvd12.ezymq.rabbitmq.codec.EzyRabbitDataCodec;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitRpcServer;
import com.tvd12.ezymq.rabbitmq.setting.EzyRabbitRpcHandlerSetting;

public class EzyRabbitRpcHandlerManager extends EzyRabbitAbstractManager {
	
	protected final EzyRabbitDataCodec dataCodec;
	protected final Map<String, EzyRabbitRpcHandler> rpcHandlers;
	protected final Map<String, EzyRabbitRpcHandlerSetting> rpcHandlerSettings;
	
	public EzyRabbitRpcHandlerManager(
			EzyRabbitDataCodec dataCodec,
			ConnectionFactory connectionFactory,
			Map<String, EzyRabbitRpcHandlerSetting> rpcHandlerSettings) {
		super(connectionFactory);
		this.dataCodec = dataCodec;
		this.rpcHandlerSettings = rpcHandlerSettings;
		this.rpcHandlers = createRpcCallers();
	}
	
	public EzyRabbitRpcHandler getRpcHandler(String name) {
		EzyRabbitRpcHandler handler = rpcHandlers.get(name);
		if(handler == null)
			throw new IllegalArgumentException("has no rpc handler with name: " + name);
		return handler;
	}
	
	protected Map<String, EzyRabbitRpcHandler> createRpcCallers() {
		Map<String, EzyRabbitRpcHandler> map = new HashMap<>();
		for(String name : rpcHandlerSettings.keySet()) {
			EzyRabbitRpcHandlerSetting setting = rpcHandlerSettings.get(name);
			map.put(name, createRpcHandler(name, setting));
		}
		return map;
	}
	
	protected EzyRabbitRpcHandler createRpcHandler(
			String name,
			EzyRabbitRpcHandlerSetting setting) {
		try {
			return createRpcHandler(setting);
		}
		catch (Exception e) {
			throw new IllegalStateException("can't create handler: " + name, e);
		}
	}
	
	protected EzyRabbitRpcHandler createRpcHandler(
			EzyRabbitRpcHandlerSetting setting) throws Exception {
		Channel channel = getChannel(setting);
		channel.basicQos(setting.getPrefetchCount());
		EzyRabbitRpcServer client = EzyRabbitRpcServer.builder()
				.channel(channel)
				.exchange(setting.getExchange())
				.replyRoutingKey(setting.getReplyRoutingKey())
				.queueName(setting.getRequestQueueName())
				.build();
		EzyRabbitRpcHandler handler = EzyRabbitRpcHandler.builder()
				.dataCodec(dataCodec)
				.actionInterceptor(setting.getActionInterceptor())
				.requestHandlers(setting.getRequestHandlers())
				.threadPoolSize(setting.getThreadPoolSize())
				.server(client).build();
		handler.start();
		return handler;
	}
	
	public void close() {
		for(EzyRabbitRpcHandler handler : rpcHandlers.values())
			handler.stop();
	}
	
}
