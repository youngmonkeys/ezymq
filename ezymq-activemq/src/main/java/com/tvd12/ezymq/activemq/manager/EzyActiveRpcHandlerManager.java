package com.tvd12.ezymq.activemq.manager;

import java.util.HashMap;
import java.util.Map;

import com.tvd12.ezymq.activemq.EzyActiveRpcHandler;
import com.tvd12.ezymq.activemq.codec.EzyActiveDataCodec;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveRpcServer;
import com.tvd12.ezymq.activemq.setting.EzyActiveRpcHandlerSetting;

public class EzyActiveRpcHandlerManager {
	
	protected final EzyActiveDataCodec dataCodec;
	protected final Map<String, EzyActiveRpcHandler> rpcHandlers;
	protected final Map<String, EzyActiveRpcHandlerSetting> rpcHandlerSettings;
	
	public EzyActiveRpcHandlerManager(
			EzyActiveDataCodec dataCodec,
			Map<String, EzyActiveRpcHandlerSetting> rpcHandlerSettings) {
		this.dataCodec = dataCodec;
		this.rpcHandlerSettings = rpcHandlerSettings;
		this.rpcHandlers = createRpcCallers();
	}
	
	public EzyActiveRpcHandler getRpcHandler(String name) {
		EzyActiveRpcHandler handler = rpcHandlers.get(name);
		if(handler == null)
			throw new IllegalArgumentException("has no rpc handler with name: " + name);
		return handler;
	}
	
	protected Map<String, EzyActiveRpcHandler> createRpcCallers() {
		Map<String, EzyActiveRpcHandler> map = new HashMap<>();
		for(String name : rpcHandlerSettings.keySet()) {
			EzyActiveRpcHandlerSetting setting = rpcHandlerSettings.get(name);
			map.put(name, createRpcCaller(name, setting));
		}
		return map;
	}
	
	protected EzyActiveRpcHandler createRpcCaller(
			String name,
			EzyActiveRpcHandlerSetting setting) {
		try {
			return createRpcCaller(setting);
		}
		catch (Exception e) {
			throw new IllegalStateException("can't create handler: " + name, e);
		}
	}
	
	protected EzyActiveRpcHandler createRpcCaller(
			EzyActiveRpcHandlerSetting setting) throws Exception {
		EzyActiveRpcServer client = EzyActiveRpcServer.builder()
				.session(setting.getSession())
				.threadPoolSize(setting.getThreadPoolSize())
				.requestQueueName(setting.getReplyQueueName())
				.requestQueue(setting.getRequestQueue())
				.replyQueueName(setting.getReplyQueueName())
				.replyQueue(setting.getReplyQueue())
				.build();
		EzyActiveRpcHandler handler = EzyActiveRpcHandler.builder()
				.dataCodec(dataCodec)
				.actionInterceptor(setting.getActionInterceptor())
				.server(client).build();
		handler.start();
		return handler;
	}
	
}
