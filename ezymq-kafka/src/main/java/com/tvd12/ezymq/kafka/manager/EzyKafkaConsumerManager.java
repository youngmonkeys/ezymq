package com.tvd12.ezymq.kafka.manager;

import java.util.HashMap;
import java.util.Map;

import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezymq.kafka.EzyKafkaConsumer;
import com.tvd12.ezymq.kafka.codec.EzyKafkaDataCodec;
import com.tvd12.ezymq.kafka.endpoint.EzyKafkaServer;
import com.tvd12.ezymq.kafka.setting.EzyKafkaConsumerSetting;

public class EzyKafkaConsumerManager 
		extends EzyKafkaAbstractManager implements EzyCloseable {
	
	protected final EzyKafkaDataCodec dataCodec;
	protected final Map<String, EzyKafkaConsumer> consumers;
	protected final Map<String, EzyKafkaConsumerSetting> consumerSettings;
	
	public EzyKafkaConsumerManager(
			EzyKafkaDataCodec dataCodec,
			Map<String, EzyKafkaConsumerSetting> consumerSettings) {
		this.dataCodec = dataCodec;
		this.consumerSettings = consumerSettings;
		this.consumers = createRpcConsumers();
	}
	
	public EzyKafkaConsumer getConsumer(String name) {
		EzyKafkaConsumer consumer = consumers.get(name);
		if(consumer == null)
			throw new IllegalArgumentException("has no consumer with name: " + name);
		return consumer;
	}
	
	protected Map<String, EzyKafkaConsumer> createRpcConsumers() {
		Map<String, EzyKafkaConsumer> map = new HashMap<>();
		for(String name : consumerSettings.keySet()) {
			EzyKafkaConsumerSetting setting = consumerSettings.get(name);
			map.put(name, createConsumer(name, setting));
		}
		return map;
	}
	
	protected EzyKafkaConsumer createConsumer(
			String name,
			EzyKafkaConsumerSetting setting) {
		try {
			return createConsumer(setting);
		}
		catch (Exception e) {
			throw new IllegalStateException("can't create handler: " + name, e);
		}
	}
	
	protected EzyKafkaConsumer createConsumer(
			EzyKafkaConsumerSetting setting) throws Exception {
		EzyKafkaServer server = EzyKafkaServer.builder()
				.topic(setting.getTopic())
				.consumer(setting.getConsumer())
				.pollTimeOut(setting.getPollTimeOut())
				.threadPoolSize(setting.getThreadPoolSize())
				.properties(setting.getProperties())
				.build();
		EzyKafkaConsumer consumer = EzyKafkaConsumer.builder()
				.dataCodec(dataCodec)
				.messageInterceptor(setting.getMessageInterceptor())
				.messageHandlers(setting.getMessageHandlers())
				.server(server)
				.build();
		return consumer;
	}
	
	@Override
	public void close() {
		for(EzyKafkaConsumer consumer : consumers.values())
			consumer.close();
	}
	
}
