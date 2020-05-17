package com.tvd12.ezymq.rabbitmq.manager;

import java.util.HashMap;
import java.util.Map;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezymq.rabbitmq.EzyRabbitRpcCaller;
import com.tvd12.ezymq.rabbitmq.constant.EzyRabbitExchangeTypes;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitRpcClient;
import com.tvd12.ezymq.rabbitmq.setting.EzyRabbitRpcCallerSetting;

public class EzyRabbitRpcCallerManager extends EzyRabbitAbstractManager {
	
	protected final EzyEntityCodec entityCodec;
	protected final Map<String, EzyRabbitRpcCaller> rpcCallers;
	protected final Map<String, EzyRabbitRpcCallerSetting> rpcCallerSettings;
	
	public EzyRabbitRpcCallerManager(
			EzyEntityCodec entityCodec,
			ConnectionFactory connectionFactory,
			Map<String, EzyRabbitRpcCallerSetting> rpcCallerSettings) {
		super(connectionFactory);
		this.entityCodec = entityCodec;
		this.rpcCallerSettings = rpcCallerSettings;
		this.rpcCallers = createRpcCallers();
	}
	
	public EzyRabbitRpcCaller getRpcCaller(String name) {
		EzyRabbitRpcCaller caller = rpcCallers.get(name);
		if(caller == null)
			throw new IllegalArgumentException("has no rpc caller with name: " + name);
		return caller;
	}
	
	protected Map<String, EzyRabbitRpcCaller> createRpcCallers() {
		Map<String, EzyRabbitRpcCaller> map = new HashMap<>();
		for(String name : rpcCallerSettings.keySet()) {
			EzyRabbitRpcCallerSetting setting = rpcCallerSettings.get(name);
			map.put(name, createRpcCaller(name, setting));
		}
		return map;
	}
	
	protected EzyRabbitRpcCaller 
			createRpcCaller(String name, EzyRabbitRpcCallerSetting setting) {
		try {
			return createRpcCaller(setting);
		}
		catch (Exception e) {
			throw new IllegalStateException("create rpc caller: " + name + " error", e);
		}
	}
	
	protected EzyRabbitRpcCaller 
			createRpcCaller(EzyRabbitRpcCallerSetting setting) throws Exception {
		Channel channel = getChannel(setting);
		declareComponents(channel, setting);
		EzyRabbitRpcClient client = EzyRabbitRpcClient.builder()
				.channel(channel)
				.exchange(setting.getExchange())
				.routingKey(setting.getRequestRoutingKey())
				.replyQueueName(setting.getReplyQueueName())
				.replyRoutingKey(setting.getReplyRoutingKey())
				.capacity(setting.getCapacity())
				.defaultTimeout(setting.getDefaultTimeout())
				.correlationIdFactory(setting.getCorrelationIdFactory())
				.unconsumedResponseConsumer(setting.getUnconsumedResponseConsumer())
				.build();
		EzyRabbitRpcCaller caller = EzyRabbitRpcCaller.builder()
				.entityCodec(entityCodec)
				.client(client).build();
		return caller;
	}
	
	protected void declareComponents(
			Channel channel, EzyRabbitRpcCallerSetting setting) throws Exception {
		channel.basicQos(1);
		channel.exchangeDeclare(setting.getExchange(), EzyRabbitExchangeTypes.DIRECT);
		channel.queueDeclare(setting.getRequestQueueName(), false, false, false, null);
		channel.queueDeclare(setting.getReplyQueueName(), false, false, false, null);
		channel.queueBind(setting.getRequestQueueName(), setting.getExchange(), setting.getRequestRoutingKey());
		channel.queueBind(setting.getReplyQueueName(), setting.getExchange(), setting.getReplyRoutingKey());
	}
	
}
