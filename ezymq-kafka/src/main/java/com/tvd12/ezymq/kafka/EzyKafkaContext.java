package com.tvd12.ezymq.kafka;

import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezymq.kafka.codec.EzyKafkaDataCodec;
import com.tvd12.ezymq.kafka.manager.EzyKafkaCallerManager;
import com.tvd12.ezymq.kafka.manager.EzyKafkaHandlerManager;
import com.tvd12.ezymq.kafka.setting.EzyKafkaSettings;

public class EzyKafkaContext implements EzyCloseable {

	protected final EzyKafkaSettings settings;
	protected final EzyEntityCodec entityCodec;
	protected final EzyKafkaDataCodec dataCodec;
	protected final EzyKafkaCallerManager callerManager;
	protected final EzyKafkaHandlerManager handlerManager;
	
	public EzyKafkaContext(
			EzyEntityCodec entityCodec,
			EzyKafkaDataCodec dataCodec,
			EzyKafkaSettings settings) {
		this.settings = settings;
		this.dataCodec = dataCodec;
		this.entityCodec = entityCodec;
		this.callerManager = newCallerManager();
		this.handlerManager = newKafkaHandlerManager();
		
	}
	
	public EzyKafkaCaller getCaller(String name) {
		return callerManager.getCaller(name);
	}
	
	public EzyKafkaHandler getKafkaHandler(String name) {
		return handlerManager.getHandler(name);
	}
	
	@Override
	public void close() {
		callerManager.close();
		handlerManager.close();
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
