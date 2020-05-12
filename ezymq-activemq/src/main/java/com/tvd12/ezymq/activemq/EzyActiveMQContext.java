package com.tvd12.ezymq.activemq;

import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezymq.activemq.codec.EzyActiveDataCodec;
import com.tvd12.ezymq.activemq.manager.EzyActiveRpcCallerManager;
import com.tvd12.ezymq.activemq.manager.EzyActiveRpcHandlerManager;
import com.tvd12.ezymq.activemq.manager.EzyActiveTopicManager;
import com.tvd12.ezymq.activemq.setting.EzyActiveSettings;

public class EzyActiveMQContext {

	protected final EzyActiveSettings settings;
	protected final EzyEntityCodec entityCodec;
	protected final EzyActiveDataCodec dataCodec;
	protected final EzyActiveTopicManager topicManager;
	protected final EzyActiveRpcCallerManager rpcCallerManager;
	protected final EzyActiveRpcHandlerManager rpcHandlerManager;
	
	public EzyActiveMQContext(EzyActiveSettings settings) {
		this.settings = settings;
		this.dataCodec = settings.getDataCodec();
		this.entityCodec = settings.getEntityCodec();
		this.topicManager = newTopicManager();
		this.rpcCallerManager = newRpcCallerManager();
		this.rpcHandlerManager = newActiveRpcHandlerManager();
		
	}
	
	public <T> EzyActiveTopic<T> getTopic(String name) {
		return topicManager.getTopic(name);
	}
	
	public EzyActiveRpcCaller getRpcCaller(String name) {
		return rpcCallerManager.getRpcCaller(name);
	}
	
	public EzyActiveRpcHandler getActiveRpcHandler(String name) {
		return rpcHandlerManager.getRpcHandler(name);
	}
	
	protected EzyActiveTopicManager newTopicManager() {
		return new EzyActiveTopicManager(
				settings.getDataCodec(),
				settings.getTopicSettings()
		);
	}
	
	protected EzyActiveRpcCallerManager newRpcCallerManager() {
		return new EzyActiveRpcCallerManager(
				settings.getEntityCodec(),
				settings.getRpcCallerSettings()
		);
	}
	
	protected EzyActiveRpcHandlerManager newActiveRpcHandlerManager() {
		return new EzyActiveRpcHandlerManager(
				settings.getDataCodec(),
				settings.getRpcHandlerSettings()
		);
	}
	
}
