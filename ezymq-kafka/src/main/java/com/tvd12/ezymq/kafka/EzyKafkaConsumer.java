package com.tvd12.ezymq.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezyfox.util.EzyLoggable;
import com.tvd12.ezyfox.util.EzyStartable;
import com.tvd12.ezymq.kafka.codec.EzyKafkaDataCodec;
import com.tvd12.ezymq.kafka.endpoint.EzyKafkaServer;
import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageInterceptor;
import com.tvd12.ezymq.kafka.handler.EzyKafkaRecordsHandler;
import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageHandlers;

import lombok.Setter;

@SuppressWarnings("rawtypes")
public class EzyKafkaConsumer
		extends EzyLoggable
		implements EzyKafkaRecordsHandler, EzyStartable, EzyCloseable {

	@Setter
	protected EzyKafkaMessageInterceptor messageInterceptor;
	
	protected final EzyKafkaServer server;
	protected final EzyKafkaDataCodec dataCodec;
	protected final EzyKafkaMessageHandlers messageHandlers;
	
	public EzyKafkaConsumer(
			EzyKafkaServer server,
			EzyKafkaDataCodec dataCodec,
			EzyKafkaMessageHandlers messageHandlers) {
		this.server = server;
		this.server.setRecordsHandler(this);
		this.dataCodec = dataCodec;
		this.messageHandlers = messageHandlers;
	}
	
	@Override
	public void start() throws Exception {
		server.start();
	}
	
	@Override
	public void close() {
		server.close();
	}
	
	@Override
	public void handleRecord(ConsumerRecord record) {
		String cmd = "";
		String topic = record.topic();
		Object key = record.key();
		if(key != null)
			cmd = new String((byte[])key);
        Object message = null;
        Object result = null;
        try {
        	byte[] requestBody = (byte[])record.value();
        	message = dataCodec.deserialize(topic, cmd, requestBody);
            if (messageInterceptor != null)
                messageInterceptor.preHandle(topic, cmd, message);
            result = messageHandlers.handle(cmd, message);
            if (messageInterceptor != null)
                messageInterceptor.postHandle(topic, cmd, message, result);
        }
        catch (Throwable e) {
        	if (messageInterceptor != null)
                messageInterceptor.postHandle(topic, cmd, message, e);
        	else
        		logger.warn("handle command: {}, message: {} error", cmd, message, e);
        }
	}
	
	public static Builder builder() {
		return new Builder();
	}
	
	public static class Builder implements EzyBuilder<EzyKafkaConsumer> {
		
		protected EzyKafkaServer server;
		protected EzyKafkaDataCodec dataCodec;
		protected EzyKafkaMessageHandlers messageHandlers;
		protected EzyKafkaMessageInterceptor messageInterceptor;
		
		public Builder server(EzyKafkaServer server) {
			this.server = server;
			return this;
		}
		
		public Builder dataCodec(EzyKafkaDataCodec dataCodec) {
			this.dataCodec = dataCodec;
			return this;
		}
		
		public Builder messageHandlers(EzyKafkaMessageHandlers messageHandlers) {
			this.messageHandlers = messageHandlers;
			return this;
		}
		
		public Builder messageInterceptor(EzyKafkaMessageInterceptor messageInterceptor) {
			this.messageInterceptor = messageInterceptor;
			return this;
		}
		
		@Override
		public EzyKafkaConsumer build() {
			EzyKafkaConsumer handler = new EzyKafkaConsumer(server, dataCodec, messageHandlers);
			handler.setMessageInterceptor(messageInterceptor);
			return handler;
		}
		
	}
	
}
