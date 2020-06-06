package com.tvd12.ezymq.activemq.endpoint;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import com.tvd12.ezyfox.util.EzyStartable;
import com.tvd12.ezymq.activemq.handler.EzyActiveRpcCallHandler;
import com.tvd12.ezymq.activemq.util.EzyActiveProperties;

import lombok.Setter;

public class EzyActiveRpcServer 
		extends EzyActiveRpcEndpoint implements EzyStartable {
	
	@Setter
	protected EzyActiveRpcCallHandler callHandler;
	
	public EzyActiveRpcServer(
			Session session, 
			Destination requestQueue, 
			Destination replyQueue, 
			int threadPoolSize) throws Exception {
		super(session, requestQueue, replyQueue, threadPoolSize);
	}
	
	@Override
	protected MessageProducer createProducer() throws Exception {
		return session.createProducer(replyQueue);
	}
	
	@Override
	protected MessageConsumer createConsumer() throws Exception {
		return session.createConsumer(requestQueue);
	}

	@Override
	public void start() throws Exception {
		this.active = true;
		this.executorService.execute();
	}

	@Override
	protected void handleLoopOne() {
		BytesMessage message = null;
		try {
			message = (BytesMessage) consumer.receive();
			if(message != null)
				processRequest(message);
		}
		catch (Exception e) {
			logger.warn("handle message: {} error", message, e);
		}
	}
	
	public void processRequest(BytesMessage message) throws Exception {
        EzyActiveProperties requestProperties = getMessageProperties(message);
        byte[] requestBody = getMessageBody(message);
        String correlationId = requestProperties.getCorrelationId();
        if (correlationId != null) {
            EzyActiveProperties.Builder replyPropertiesBuilder = new EzyActiveProperties.Builder();
            byte[] replyBody = handleCall(requestProperties, requestBody, replyPropertiesBuilder);
            replyPropertiesBuilder.correlationId(correlationId);
            EzyActiveProperties replyProperties = replyPropertiesBuilder.build();
            publish(replyProperties, replyBody);
        } 
        else {
        	handleFire(requestProperties, requestBody);
        }
	}
	
	protected void handleFire(
			EzyActiveProperties requestProperties, byte[] requestBody) {
		callHandler.handleFire(requestProperties, requestBody);
	}
	
	protected byte[] handleCall(
			EzyActiveProperties requestProperties,
			byte[] requestBody, 
			EzyActiveProperties.Builder replyPropertiesBuilder) {
		byte[] answer = callHandler.handleCall(
				requestProperties, requestBody, replyPropertiesBuilder);
		return answer;
	}
	
	@Override
	protected String getThreadName() {
		return "rpc-server";
	}
	
	public static Builder builder() {
		return new Builder();
	}
	
	public static class Builder extends EzyActiveRpcEndpoint.Builder<Builder> {

		protected EzyActiveRpcCallHandler callHandler = null;
		
		public Builder callHandler(EzyActiveRpcCallHandler callHandler) {
			this.callHandler = callHandler;
			return this;
		}
		
		@Override
		public EzyActiveRpcServer build() {
			return (EzyActiveRpcServer) super.build();
		}
		
		@Override
		protected EzyActiveRpcEndpoint newProduct() throws Exception {
			return new EzyActiveRpcServer(
					session, requestQueue, replyQueue, threadPoolSize);
		}
		
	}
	
}
