package com.tvd12.ezymq.activemq.endpoint;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;

import com.tvd12.ezymq.activemq.util.EzyActiveProperties;

public class EzyActiveTopicClient extends EzyActiveTopicEndpoint {

	protected final MessageProducer producer;
	
	public EzyActiveTopicClient(
			Session session, 
			Destination topic) throws Exception {
        super(session, topic);
        this.producer = session.createProducer(topic);
    }
	
	public void publish(EzyActiveProperties props, byte[] message) 
			throws Exception {
		BytesMessage data = session.createBytesMessage();
		setMessageProperties(data, props);
		data.writeBytes(message);
		this.producer.send(data);
	}
	
	public static Builder builder() {
		return new Builder();
	}
	
	public static class Builder extends EzyActiveTopicEndpoint.Builder<Builder> {

		@Override
		public EzyActiveTopicClient build() {
			return (EzyActiveTopicClient)super.build();
		}
		
		@Override
		protected EzyActiveTopicEndpoint newEnpoint() throws Exception {
			return new EzyActiveTopicClient(session, topic);
		}
		
	}
	
}
