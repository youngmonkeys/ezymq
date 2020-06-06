package com.tvd12.ezymq.activemq.manager;

import java.util.HashMap;
import java.util.Map;

import javax.jms.ConnectionFactory;
import javax.jms.Session;

import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezymq.activemq.EzyActiveTopic;
import com.tvd12.ezymq.activemq.codec.EzyActiveDataCodec;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveTopicClient;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveTopicServer;
import com.tvd12.ezymq.activemq.setting.EzyActiveTopicSetting;

@SuppressWarnings({"rawtypes", "unchecked"})
public class EzyActiveTopicManager 
		extends EzyActiveAbstractManager implements EzyCloseable {

	protected final EzyActiveDataCodec dataCodec;
	protected final Map<String, EzyActiveTopic> topics;
	protected final Map<String, EzyActiveTopicSetting> topicSettings;
	
	public EzyActiveTopicManager(
			EzyActiveDataCodec dataCodec,
			ConnectionFactory connectionFactory,
			Map<String, EzyActiveTopicSetting> topicSettings) {
		super(connectionFactory);
		this.dataCodec = dataCodec;
		this.topicSettings = topicSettings;
		this.topics = createTopics();
	}
	
	public <T> EzyActiveTopic<T> getTopic(String name) {
		EzyActiveTopic<T> topic = topics.get(name);
		if(topic == null)
			throw new IllegalArgumentException("has no topic with name: " + name);
		return topic;
	}
	
	protected Map<String, EzyActiveTopic> createTopics() {
		Map<String, EzyActiveTopic> map = new HashMap<>();
		for(String name : topicSettings.keySet()) {
			EzyActiveTopicSetting setting = topicSettings.get(name);
			map.put(name, createTopic(name, setting));
		}
		return map;
	}
	
	protected EzyActiveTopic 
			createTopic(String name, EzyActiveTopicSetting setting) {
		try {
			return createTopic(setting);
		}
		catch (Exception e) {
			throw new IllegalStateException("can't create topic: " + name, e);
		}
	}
	
	protected EzyActiveTopic 
			createTopic(EzyActiveTopicSetting setting) throws Exception {
		EzyActiveTopicClient client = null;
		EzyActiveTopicServer server = null;
		Session session = getSession(setting);
		if(setting.isClientEnable()) {
			client = EzyActiveTopicClient.builder()
					.session(session)
					.topic(setting.getTopic())
					.topicName(setting.getTopicName())
					.build();
		}
		if(setting.isServerEnable()) {
			server = EzyActiveTopicServer.builder()
					.session(session)
					.topic(setting.getTopic())
					.topicName(setting.getTopicName())
					.build();
		}
		return EzyActiveTopic.builder()
				.dataCodec(dataCodec)
				.client(client)
				.server(server).build();		
	}
	
	@Override
	public void close() {
		for(EzyActiveTopic topic : topics.values())
			topic.close();
	}
	
}
