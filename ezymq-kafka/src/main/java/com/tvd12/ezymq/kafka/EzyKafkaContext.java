package com.tvd12.ezymq.kafka;

import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezymq.kafka.codec.EzyKafkaDataCodec;
import com.tvd12.ezymq.kafka.manager.EzyKafkaCallerManager;
import com.tvd12.ezymq.kafka.manager.EzyKafkaHandlerManager;
import com.tvd12.ezymq.kafka.setting.EzyKafkaSettings;

public class EzyKafkaContext {

	protected final EzyKafkaSettings settings;
	protected final EzyEntityCodec entityCodec;
	protected final EzyKafkaDataCodec dataCodec;
	protected final EzyKafkaCallerManager rpcCallerManager;
	protected final EzyKafkaHandlerManager rpcHandlerManager;
	
	public EzyKafkaContext(
			EzyEntityCodec entityCodec,
			EzyKafkaDataCodec dataCodec,
			EzyKafkaSettings settings) {
		this.settings = settings;
		this.dataCodec = dataCodec;
		this.entityCodec = entityCodec;
		this.rpcCallerManager = newCallerManager();
		this.rpcHandlerManager = newKafkaHandlerManager();
		
	}
	
	public EzyKafkaCaller getCaller(String name) {
		return rpcCallerManager.getCaller(name);
	}
	
	public EzyKafkaHandler getKafkaHandler(String name) {
		return rpcHandlerManager.getHandler(name);
	}
	
	protected EzyKafkaCallerManager newCallerManager() {
		return new EzyKafkaCallerManager(
				entityCodec,
				settings.getCallerSettings()
		);
	}
	
	protected EzyKafkaHandlerManager newKafkaHandlerManager() {
		return new EzyKafkaHandlerManager(
				dataCodec,
				settings.getHandlerSettings()
		);
	}
	
	public static EzyKafkaContextBuilder builder() {
		return new EzyKafkaContextBuilder();
	}
	
}
