package com.tvd12.ezymq.kafka.manager;

import java.util.HashMap;
import java.util.Map;

import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezymq.kafka.EzyKafkaHandler;
import com.tvd12.ezymq.kafka.codec.EzyKafkaDataCodec;
import com.tvd12.ezymq.kafka.endpoint.EzyKafkaServer;
import com.tvd12.ezymq.kafka.setting.EzyKafkaHandlerSetting;

public class EzyKafkaHandlerManager 
		extends EzyKafkaAbstractManager implements EzyCloseable {
	
	protected final EzyKafkaDataCodec dataCodec;
	protected final Map<String, EzyKafkaHandler> handlers;
	protected final Map<String, EzyKafkaHandlerSetting> handlerSettings;
	
	public EzyKafkaHandlerManager(
			EzyKafkaDataCodec dataCodec,
			Map<String, EzyKafkaHandlerSetting> rpcHandlerSettings) {
		this.dataCodec = dataCodec;
		this.handlerSettings = rpcHandlerSettings;
		this.handlers = createRpcCallers();
	}
	
	public EzyKafkaHandler getHandler(String name) {
		EzyKafkaHandler handler = handlers.get(name);
		if(handler == null)
			throw new IllegalArgumentException("has no rpc handler with name: " + name);
		return handler;
	}
	
	protected Map<String, EzyKafkaHandler> createRpcCallers() {
		Map<String, EzyKafkaHandler> map = new HashMap<>();
		for(String name : handlerSettings.keySet()) {
			EzyKafkaHandlerSetting setting = handlerSettings.get(name);
			map.put(name, createHandler(name, setting));
		}
		return map;
	}
	
	protected EzyKafkaHandler createHandler(
			String name,
			EzyKafkaHandlerSetting setting) {
		try {
			return createHandler(setting);
		}
		catch (Exception e) {
			throw new IllegalStateException("can't create handler: " + name, e);
		}
	}
	
	protected EzyKafkaHandler createHandler(
			EzyKafkaHandlerSetting setting) throws Exception {
		EzyKafkaServer client = EzyKafkaServer.builder()
				.topic(setting.getTopic())
				.consumer(setting.getConsumer())
				.pollTimeOut(setting.getPollTimeOut())
				.threadPoolSize(setting.getThreadPoolSize())
				.properties(setting.getProperties())
				.build();
		EzyKafkaHandler handler = EzyKafkaHandler.builder()
				.dataCodec(dataCodec)
				.actionInterceptor(setting.getActionInterceptor())
				.requestHandlers(setting.getRequestHandlers())
				.server(client).build();
		handler.start();
		return handler;
	}
	
	@Override
	public void close() {
		for(EzyKafkaHandler handler : handlers.values())
			handler.close();
	}
	
}
