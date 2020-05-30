package com.tvd12.ezymq.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.util.EzyLoggable;
import com.tvd12.ezyfox.util.EzyStartable;
import com.tvd12.ezyfox.util.EzyStoppable;
import com.tvd12.ezymq.kafka.codec.EzyKafkaDataCodec;
import com.tvd12.ezymq.kafka.endpoint.EzyKafkaServer;
import com.tvd12.ezymq.kafka.handler.EzyKafkaActionInterceptor;
import com.tvd12.ezymq.kafka.handler.EzyKafkaRecordsHandler;
import com.tvd12.ezymq.kafka.handler.EzyKafkaRequestHandlers;

import lombok.Setter;

@SuppressWarnings("rawtypes")
public class EzyKafkaHandler
		extends EzyLoggable
		implements EzyKafkaRecordsHandler, EzyStartable, EzyStoppable {

	@Setter
	protected EzyKafkaActionInterceptor actionInterceptor;
	
	protected final EzyKafkaServer server;
	protected final EzyKafkaDataCodec dataCodec;
	protected final EzyKafkaRequestHandlers requestHandlers;
	
	public EzyKafkaHandler(
			EzyKafkaServer server,
			EzyKafkaDataCodec dataCodec,
			EzyKafkaRequestHandlers requestHandlers) {
		this.server = server;
		this.server.setRecordsHandler(this);
		this.dataCodec = dataCodec;
		this.requestHandlers = requestHandlers;
	}
	
	@Override
	public void start() throws Exception {
		server.start();
	}
	
	@Override
	public void stop() {
		server.stop();
	}
	
	@Override
	public void handleRecord(ConsumerRecord record) {
		String cmd = record.topic();
        Object requestEntity = null;
        Object responseEntity = null;
        try {
        	byte[] requestBody = (byte[])record.value();
            requestEntity = dataCodec.deserialize(cmd, requestBody);
            if (actionInterceptor != null)
                actionInterceptor.intercept(cmd, requestEntity);
            responseEntity = requestHandlers.handle(cmd, requestEntity);
            if (actionInterceptor != null)
                actionInterceptor.intercept(cmd, requestEntity, responseEntity);
        }
        catch (Exception e) {
        	if (actionInterceptor != null)
                actionInterceptor.intercept(cmd, requestEntity, e);
        }
	}
	
	public static Builder builder() {
		return new Builder();
	}
	
	public static class Builder implements EzyBuilder<EzyKafkaHandler> {
		
		protected EzyKafkaServer server;
		protected EzyKafkaDataCodec dataCodec;
		protected EzyKafkaRequestHandlers requestHandlers;
		protected EzyKafkaActionInterceptor actionInterceptor;
		
		public Builder server(EzyKafkaServer server) {
			this.server = server;
			return this;
		}
		
		public Builder dataCodec(EzyKafkaDataCodec dataCodec) {
			this.dataCodec = dataCodec;
			return this;
		}
		
		public Builder requestHandlers(EzyKafkaRequestHandlers requestHandlers) {
			this.requestHandlers = requestHandlers;
			return this;
		}
		
		public Builder actionInterceptor(EzyKafkaActionInterceptor actionInterceptor) {
			this.actionInterceptor = actionInterceptor;
			return this;
		}
		
		@Override
		public EzyKafkaHandler build() {
			EzyKafkaHandler handler = new EzyKafkaHandler(server, dataCodec, requestHandlers);
			handler.setActionInterceptor(actionInterceptor);
			return handler;
		}
		
	}
	
}
