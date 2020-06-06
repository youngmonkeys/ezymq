package com.tvd12.ezymq.activemq.endpoint;

import javax.jms.Destination;
import javax.jms.Session;

import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezymq.activemq.constant.EzyActiveDestinationType;

public abstract class EzyActiveTopicEndpoint 
		extends EzyActiveEndpoint implements EzyCloseable {

    protected final Destination topic;
	
	public EzyActiveTopicEndpoint(Session session, Destination topic) {
        super(session);
        this.topic = topic;
    }
	
	@SuppressWarnings("unchecked")
	public abstract static class Builder<B extends Builder<B>> extends EzyActiveEndpoint.Builder<B> {

		protected String topicName;
		protected Destination topic;
		
		public B topic(Destination topic) {
			this.topic = topic;
			return (B)this;
		}
		
		public B topicName(String topicName) {
			this.topicName = topicName;
			return (B)this;
		}
		
		@Override
		public EzyActiveTopicEndpoint build() {
			if(topic == null)
				topic = createDestination(EzyActiveDestinationType.TOPIC, topicName);
			try {
				return newEnpoint();
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		
		protected abstract EzyActiveTopicEndpoint newEnpoint() throws Exception;
	}
	
}
