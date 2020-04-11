package com.tvd12.ezymq.rabbitmq;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.exception.InternalServerErrorException;
import com.tvd12.ezyfox.io.EzyStrings;
import com.tvd12.ezyfox.message.EzyMessageTypeFetcher;
import com.tvd12.ezymq.rabbitmq.codec.EzyRabbitDataCodec;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitTopicClient;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitTopicServer;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitMessageConsumer;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitMessageConsumers;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitMessageHandler;

public class EzyRabbitTopic<T> {

	protected final EzyRabbitTopicClient client;
	protected final EzyRabbitTopicServer server;
	protected final EzyRabbitDataCodec dataCodec;
	protected volatile boolean consuming;
	protected EzyRabbitMessageConsumers consumers;
	
	public EzyRabbitTopic(
			EzyRabbitTopicServer server,
			EzyRabbitDataCodec dataCodec) {
		this(null, server, dataCodec);
	}
	
	public EzyRabbitTopic(
			EzyRabbitTopicClient client,
			EzyRabbitDataCodec dataCodec) {
		this(client, null, dataCodec);
	}

	public EzyRabbitTopic(
			EzyRabbitTopicClient client,
			EzyRabbitTopicServer server,
			EzyRabbitDataCodec dataCodec) {
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
		BasicProperties requestProperties = new BasicProperties.Builder()
        		.type(cmd)
        		.build();
        byte[] requestMessage = dataCodec.serialize(data);
        rawPublish(requestProperties, requestMessage);
	}
	
	protected void rawPublish(
    		BasicProperties requestProperties, byte[] requestMessage) {
    	try {
			client.publish(requestProperties, requestMessage);
		} 
    	catch (Exception e) {
    		throw new InternalServerErrorException(e.getMessage(), e);
		}
    }

	public void addConsumer(EzyRabbitMessageConsumer<T> consumer) {
		addConsumer("", consumer);
	}
	
	public void addConsumer(String cmd, EzyRabbitMessageConsumer<T> consumer) {
		if(server == null)
			throw new IllegalStateException("this topic is publishing only, set the server to consume");
		synchronized (this) {
			if(!consuming) {
				this.consuming = true;
				this.consumers = new EzyRabbitMessageConsumers();
				this.startConsuming();
			}
			consumers.addConsumer(cmd, consumer);
		}
	}

	@SuppressWarnings("unchecked")
	protected void startConsuming() {
		EzyRabbitMessageHandler messageHandler = new EzyRabbitMessageHandler() {
			@Override
			public void handle(BasicProperties requestProperties, byte[] requestBody) {
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
	public static class Builder implements EzyBuilder<EzyRabbitTopic> {
	
		protected EzyRabbitTopicClient client;
		protected EzyRabbitTopicServer server;
		protected EzyRabbitDataCodec dataCodec;
	
		public Builder client(EzyRabbitTopicClient client) {
			this.client = client;
			return this;
		}
		
		public Builder server(EzyRabbitTopicServer server) {
			this.server = server;
			return this;
		}
	
		public Builder dataCodec(EzyRabbitDataCodec dataCodec) {
			this.dataCodec = dataCodec;
			return this;
		}
	
		public EzyRabbitTopic build() {
			return new EzyRabbitTopic(client, server, dataCodec);
		}
	
	}
	
}
