package com.tvd12.ezymq.activemq;

import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.exception.InternalServerErrorException;
import com.tvd12.ezyfox.io.EzyStrings;
import com.tvd12.ezyfox.message.EzyMessageTypeFetcher;
import com.tvd12.ezymq.activemq.codec.EzyActiveDataCodec;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveTopicClient;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveTopicServer;
import com.tvd12.ezymq.activemq.handler.EzyActiveMessageConsumer;
import com.tvd12.ezymq.activemq.handler.EzyActiveMessageConsumers;
import com.tvd12.ezymq.activemq.handler.EzyActiveMessageHandler;
import com.tvd12.ezymq.activemq.util.EzyActiveProperties;

public class EzyActiveTopic<T> {

	protected final EzyActiveTopicClient client;
	protected final EzyActiveTopicServer server;
	protected final EzyActiveDataCodec dataCodec;
	protected volatile boolean consuming;
	protected EzyActiveMessageConsumers consumers;
	
	public EzyActiveTopic(
			EzyActiveTopicServer server,
			EzyActiveDataCodec dataCodec) {
		this(null, server, dataCodec);
	}
	
	public EzyActiveTopic(
			EzyActiveTopicClient client,
			EzyActiveDataCodec dataCodec) {
		this(client, null, dataCodec);
	}

	public EzyActiveTopic(
			EzyActiveTopicClient client,
			EzyActiveTopicServer server,
			EzyActiveDataCodec dataCodec) {
		this.client = client;
		this.server = server;
		this.dataCodec = dataCodec;
	}

	public void publish(Object data) {
		String cmd = "";
		if (data instanceof EzyMessageTypeFetcher)
			cmd = ((EzyMessageTypeFetcher)data).getMessageType();
        publish(cmd, data);
	}
	
	public void publish(String cmd, Object data) {
		if(client == null)
			throw new IllegalStateException("this topic is consuming only, set the client to publish");
		EzyActiveProperties requestProperties = new EzyActiveProperties.Builder()
        		.type(cmd)
        		.build();
        byte[] requestMessage = dataCodec.serialize(data);
        rawPublish(requestProperties, requestMessage);
	}
	
	protected void rawPublish(
			EzyActiveProperties requestProperties, byte[] requestMessage) {
    	try {
			client.publish(requestProperties, requestMessage);
		} 
    	catch (Exception e) {
    		throw new InternalServerErrorException(e.getMessage(), e);
		}
    }

	public void addConsumer(EzyActiveMessageConsumer<T> consumer) {
		addConsumer("", consumer);
	}
	
	public void addConsumer(String cmd, EzyActiveMessageConsumer<T> consumer) {
		if(server == null)
			throw new IllegalStateException("this topic is publishing only, set the server to consume");
		synchronized (this) {
			if(!consuming) {
				this.consuming = true;
				this.consumers = new EzyActiveMessageConsumers();
				this.startConsuming();
			}
			consumers.addConsumer(cmd, consumer);
		}
	}

	@SuppressWarnings("unchecked")
	protected void startConsuming() {
		EzyActiveMessageHandler messageHandler = new EzyActiveMessageHandler() {
			@Override
			public void handle(EzyActiveProperties requestProperties, byte[] requestBody) {
				String cmd = requestProperties.getType();
				if(EzyStrings.isNoContent(cmd))
					cmd = "";
				T message = (T)dataCodec.deserialize(cmd, requestBody);
				consumers.consume(cmd, message);
			}
		};
		server.setMessageHandler(messageHandler);
		try {
			server.start();
		}
		catch (Exception e) {
			throw new IllegalStateException("can't start topic server");
		}
	}
	
	public static Builder builder() {
		return new Builder();
	}

	@SuppressWarnings("rawtypes")
	public static class Builder implements EzyBuilder<EzyActiveTopic> {
	
		protected EzyActiveTopicClient client;
		protected EzyActiveTopicServer server;
		protected EzyActiveDataCodec dataCodec;
	
		public Builder client(EzyActiveTopicClient client) {
			this.client = client;
			return this;
		}
		
		public Builder server(EzyActiveTopicServer server) {
			this.server = server;
			return this;
		}
	
		public Builder dataCodec(EzyActiveDataCodec dataCodec) {
			this.dataCodec = dataCodec;
			return this;
		}
	
		public EzyActiveTopic build() {
			return new EzyActiveTopic(client, server, dataCodec);
		}
	
	}
	
}
