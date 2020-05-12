package com.tvd12.ezymq.activemq.setting;

import javax.jms.Destination;
import javax.jms.Session;

import lombok.Getter;

@Getter
public class EzyActiveTopicSetting extends EzyActiveEndpointSetting {

	protected final String topicName;
	protected final Destination topic;
	protected final boolean clientEnable;
	protected final boolean serverEnable;
	protected final int serverThreadPoolSize;
	
	public EzyActiveTopicSetting(
			Session session,
			String topicName,
			Destination topic,
			boolean clientEnable,
			boolean serverEnable,
			int serverThreadPoolSize) {
		super(session);
		this.topic = topic;
		this.topicName = topicName;
		this.clientEnable = clientEnable;
		this.serverEnable = serverEnable;
		this.serverThreadPoolSize = serverThreadPoolSize;
	}

}
