package com.tvd12.ezymq.activemq.endpoint;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.Session;

import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.io.EzyStrings;
import com.tvd12.ezyfox.util.EzyLoggable;
import com.tvd12.ezymq.activemq.constant.EzyActiveDestinationType;
import com.tvd12.ezymq.activemq.util.EzyActiveMessages;
import com.tvd12.ezymq.activemq.util.EzyActiveProperties;

public class EzyActiveEndpoint extends EzyLoggable {

    protected final Session session;
	
	public EzyActiveEndpoint(Session session) {
        this.session = session;
    }
	
	protected void setMessageProperties(
			Message message, EzyActiveProperties props) throws Exception {
		EzyActiveMessages.setMessageProperties(message, props);
	}
	
	protected byte[] getMessageBody(BytesMessage message) throws Exception {
		return EzyActiveMessages.getMessageBody(message);
	}
	
	protected EzyActiveProperties getMessageProperties(
			Message message) throws Exception {
		return EzyActiveMessages.getMessageProperties(message);
	}
	
	@SuppressWarnings("unchecked")
	public static abstract class Builder<B extends Builder<B>> implements EzyBuilder<EzyActiveEndpoint> {
		
		protected Session session; 
		
		public B session(Session session) {
			this.session = session;
			return (B)this;
		}
		
		protected Destination createDestination(EzyActiveDestinationType type, String name) {
			if(EzyStrings.isNoContent(name))
				throw new NullPointerException(type.getName() + " name can't be null or empty");
			try {
				if(type == EzyActiveDestinationType.QUEUE)
					return session.createQueue(name);
				return session.createTopic(name);
			}
			catch (Exception e) {
				throw new IllegalStateException("can't create " + type.getName() + " with name: " + name, e);
			}
		}

	}
	
}
