package com.tvd12.ezymq.kafka.manager;

import java.util.HashMap;
import java.util.Map;

import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezymq.kafka.EzyKafkaCaller;
import com.tvd12.ezymq.kafka.endpoint.EzyKafkaClient;
import com.tvd12.ezymq.kafka.setting.EzyKafkaCallerSetting;

public class EzyKafkaCallerManager extends EzyKafkaAbstractManager {
	
	protected final EzyEntityCodec entityCodec;
	protected final Map<String, EzyKafkaCaller> callers;
	protected final Map<String, EzyKafkaCallerSetting> callerSettings;
	
	public EzyKafkaCallerManager(
			EzyEntityCodec entityCodec,
			Map<String, EzyKafkaCallerSetting> callerSettings) {
		this.entityCodec = entityCodec;
		this.callerSettings = callerSettings;
		this.callers = createCallers();
	}
	
	public EzyKafkaCaller getCaller(String name) {
		EzyKafkaCaller caller = callers.get(name);
		if(caller == null)
			throw new IllegalArgumentException("has no caller with name: " + name);
		return caller;
	}
	
	protected Map<String, EzyKafkaCaller> createCallers() {
		Map<String, EzyKafkaCaller> map = new HashMap<>();
		for(String name : callerSettings.keySet()) {
			EzyKafkaCallerSetting setting = callerSettings.get(name);
			map.put(name, createCaller(name, setting));
		}
		return map;
	}
	
	protected EzyKafkaCaller 
			createCaller(String name, EzyKafkaCallerSetting setting) {
		try {
			return createCaller(setting);
		}
		catch (Exception e) {
			throw new IllegalStateException("create caller: " + name + " error", e);
		}
	}
	
	protected EzyKafkaCaller 
			createCaller(EzyKafkaCallerSetting setting) throws Exception {
		EzyKafkaClient client = EzyKafkaClient.builder()
				.topic(setting.getTopic())
				.producer(setting.getProducer())
				.properties(setting.getProperties())
				.build();
		EzyKafkaCaller caller = EzyKafkaCaller.builder()
				.entityCodec(entityCodec)
				.client(client).build();
		return caller;
	}
	
}
