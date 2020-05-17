package com.tvd12.ezymq.rabbitmq.manager;

import java.util.HashMap;
import java.util.Map;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.tvd12.ezymq.rabbitmq.EzyRabbitTopic;
import com.tvd12.ezymq.rabbitmq.codec.EzyRabbitDataCodec;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitTopicClient;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitTopicServer;
import com.tvd12.ezymq.rabbitmq.setting.EzyRabbitTopicSetting;

@SuppressWarnings({"rawtypes", "unchecked"})
public class EzyRabbitTopicManager extends EzyRabbitAbstractManager {

	protected final EzyRabbitDataCodec dataCodec;
	protected final Map<String, EzyRabbitTopic> topics;
	protected final Map<String, EzyRabbitTopicSetting> topicSettings;
	
	public EzyRabbitTopicManager(
			EzyRabbitDataCodec dataCodec,
			ConnectionFactory connectionFactory,
			Map<String, EzyRabbitTopicSetting> topicSettings) {
		super(connectionFactory);
		this.dataCodec = dataCodec;
		this.topicSettings = topicSettings;
		this.topics = createTopics();
	}
	
	public <T> EzyRabbitTopic<T> getTopic(String name) {
		EzyRabbitTopic<T> topic = topics.get(name);
		if(topic == null)
			throw new IllegalArgumentException("has no topic with name: " + name);
		return topic;
	}
	
	protected Map<String, EzyRabbitTopic> createTopics() {
		Map<String, EzyRabbitTopic> map = new HashMap<>();
		for(String name : topicSettings.keySet()) {
			EzyRabbitTopicSetting setting = topicSettings.get(name);
			map.put(name, createTopic(name, setting));
		}
		return map;
	}
	
	protected EzyRabbitTopic 
			createTopic(String name, EzyRabbitTopicSetting setting) {
		try {
			return createTopic(setting);
		}
		catch (Exception e) {
			throw new IllegalStateException("can't create topic: " + name, e);
		}
	}
	
	protected EzyRabbitTopic 
			createTopic(EzyRabbitTopicSetting setting) throws Exception {
		EzyRabbitTopicClient client = null;
		EzyRabbitTopicServer server = null;
		Channel channel = getChannel(setting);
		if(setting.isClientEnable()) {
			client = EzyRabbitTopicClient.builder()
					.channel(channel)
					.exchange(setting.getExchange())
					.routingKey(setting.getClientRoutingKey())
					.build();
		}
		if(setting.isServerEnable()) {
			server = EzyRabbitTopicServer.builder()
					.channel(setting.getChannel())
					.exchange(setting.getExchange())
					.queueName(setting.getServerQueueName())
					.build();
		}
		return EzyRabbitTopic.builder()
				.dataCodec(dataCodec)
				.client(client)
				.server(server).build();		
	}
	
}
