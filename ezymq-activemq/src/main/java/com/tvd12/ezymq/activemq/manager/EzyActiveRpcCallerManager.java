package com.tvd12.ezymq.activemq.manager;

import java.util.HashMap;
import java.util.Map;

import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezymq.activemq.EzyActiveRpcCaller;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveRpcClient;
import com.tvd12.ezymq.activemq.setting.EzyActiveRpcCallerSetting;

public class EzyActiveRpcCallerManager {
	
	protected final EzyEntityCodec entityCodec;
	protected final Map<String, EzyActiveRpcCaller> rpcCallers;
	protected final Map<String, EzyActiveRpcCallerSetting> rpcCallerSettings;
	
	public EzyActiveRpcCallerManager(
			EzyEntityCodec entityCodec,
			Map<String, EzyActiveRpcCallerSetting> rpcCallerSettings) {
		this.entityCodec = entityCodec;
		this.rpcCallerSettings = rpcCallerSettings;
		this.rpcCallers = createRpcCallers();
	}
	
	public EzyActiveRpcCaller getRpcCaller(String name) {
		EzyActiveRpcCaller caller = rpcCallers.get(name);
		if(caller == null)
			throw new IllegalArgumentException("has no rpc caller with name: " + name);
		return caller;
	}
	
	protected Map<String, EzyActiveRpcCaller> createRpcCallers() {
		Map<String, EzyActiveRpcCaller> map = new HashMap<>();
		for(String name : rpcCallerSettings.keySet()) {
			EzyActiveRpcCallerSetting setting = rpcCallerSettings.get(name);
			map.put(name, createRpcCaller(setting));
		}
		return map;
	}
	
	protected EzyActiveRpcCaller createRpcCaller(EzyActiveRpcCallerSetting setting) {
		EzyActiveRpcClient client = EzyActiveRpcClient.builder()
				.session(setting.getSession())
				.capacity(setting.getCapacity())
				.defaultTimeout(setting.getDefaultTimeout())
				.threadPoolSize(setting.getThreadPoolSize())
				.requestQueueName(setting.getReplyQueueName())
				.requestQueue(setting.getRequestQueue())
				.replyQueueName(setting.getReplyQueueName())
				.replyQueue(setting.getReplyQueue())
				.build();
		EzyActiveRpcCaller caller = EzyActiveRpcCaller.builder()
				.entityCodec(entityCodec)
				.client(client).build();
		return caller;
	}
	
}
