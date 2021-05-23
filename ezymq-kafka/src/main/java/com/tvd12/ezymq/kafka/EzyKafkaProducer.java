package com.tvd12.ezymq.kafka;

import java.util.concurrent.TimeoutException;

import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezyfox.exception.EzyTimeoutException;
import com.tvd12.ezyfox.exception.InternalServerErrorException;
import com.tvd12.ezyfox.message.EzyMessageTypeFetcher;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezyfox.util.EzyLoggable;
import com.tvd12.ezymq.kafka.endpoint.EzyKafkaClient;

public class EzyKafkaProducer extends EzyLoggable implements EzyCloseable {

	protected final EzyKafkaClient client;
	protected final EzyEntityCodec entityCodec;

	public EzyKafkaProducer(
			EzyKafkaClient client, EzyEntityCodec entityCodec) {
        this.client = client;
        this.entityCodec = entityCodec;
    }
	
	public void send(Object data) {
		String command = "";
        if (data instanceof EzyMessageTypeFetcher)
        	command = ((EzyMessageTypeFetcher)data).getMessageType();
        send(command, data);
    }

    public void send(String cmd, Object data) {
        byte[] requestMessage = entityCodec.serialize(data);
        rawSend(cmd, requestMessage);
    }
	
    protected void rawSend(String cmd, byte[] requestMessage) {
    		try {
			client.send(cmd, requestMessage);
		} 
    	catch (TimeoutException e) {
			throw new EzyTimeoutException("call request: " + cmd + " timeout", e);
		}
    	catch (Exception e) {
    		throw new InternalServerErrorException(e.getMessage(), e);
		}
    }
    
    @Override
    public void close() {
    	client.close();
    }
    
    public static Builder builder() {
    	return new Builder();
    }
    
    public static class Builder implements EzyBuilder<EzyKafkaProducer> {

    	protected EzyKafkaClient client;
    	protected EzyEntityCodec entityCodec;
    	
    	public Builder client(EzyKafkaClient client) {
    		this.client = client;
    		return this;
    	}
    	
    	public Builder entityCodec(EzyEntityCodec entityCodec) {
    		this.entityCodec = entityCodec;
    		return this;
    	}
    	
		@Override
		public EzyKafkaProducer build() {
			return new EzyKafkaProducer(client, entityCodec);
		}
    	
    }
    
}
