package com.tvd12.ezymq.activemq;

import java.util.Map;
import java.util.concurrent.TimeoutException;

import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezyfox.exception.BadRequestException;
import com.tvd12.ezyfox.exception.EzyTimeoutException;
import com.tvd12.ezyfox.exception.InternalServerErrorException;
import com.tvd12.ezyfox.exception.NotFoundException;
import com.tvd12.ezyfox.message.EzyMessageTypeFetcher;
import com.tvd12.ezyfox.util.EzyLoggable;
import com.tvd12.ezyfox.util.EzyStoppable;
import com.tvd12.ezymq.activemq.constant.EzyActiveKeys;
import com.tvd12.ezymq.activemq.constant.EzyActiveStatusCodes;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveMessage;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveRpcClient;
import com.tvd12.ezymq.activemq.util.EzyActiveProperties;

public class EzyActiveRpcCaller extends EzyLoggable implements EzyStoppable {
	
	protected final EzyActiveRpcClient client;
	protected final EzyEntityCodec entityCodec;

	public EzyActiveRpcCaller(
			EzyActiveRpcClient client, EzyEntityCodec entityCodec) {
        this.client = client;
        this.entityCodec = entityCodec;
    }

	@Override
	public void stop() {
		try {
			client.close();
		} catch (Exception e) {
			logger.warn("stop rpc client error", e);
		}
	}
	
	public void fire(Object data) {
		if (!(data instanceof EzyMessageTypeFetcher))
            throw new IllegalArgumentException("data class must implement 'EzyMessageTypeFetcher'");
        EzyMessageTypeFetcher mdata = (EzyMessageTypeFetcher)data;
        fire(mdata.getMessageType(), data);
	}
	
	public void fire(String cmd, Object data) {
		EzyActiveProperties requestProperties = new EzyActiveProperties.Builder()
        		.type(cmd)
        		.build();
        byte[] requestMessage = entityCodec.serialize(data);
        rawFire(requestProperties, requestMessage);
	}
	
	public <T> T call(Object data, Class<T> returnType) {
        if (!(data instanceof EzyMessageTypeFetcher))
            throw new IllegalArgumentException("data class must implement 'EzyMessageTypeFetcher'");
        EzyMessageTypeFetcher mdata = (EzyMessageTypeFetcher)data;
        return call(mdata.getMessageType(), data, returnType);
    }

    public <T> T call(String cmd, Object data, Class<T> returnType) {
        EzyActiveProperties requestProperties = new EzyActiveProperties.Builder()
        		.type(cmd)
        		.build();
        byte[] requestMessage = entityCodec.serialize(data);
        EzyActiveMessage responseData = rawCall(requestProperties, requestMessage);
        EzyActiveProperties responseProperties = responseData.getProperties();
        processResponseProperties(responseProperties.getProperties());
        byte[] responseBody = responseData.getBody();
        T responseEntity = entityCodec.deserialize(responseBody, returnType);
        return responseEntity;
    }
    
    protected void processResponseProperties(Map<String, Object> responseHeaders) {
		if(responseHeaders == null)
			return;
		Integer status = (Integer)responseHeaders.get(EzyActiveKeys.STATUS);
		if(status == null)
			return;
        String message = responseHeaders.get(EzyActiveKeys.MESSAGE).toString();
        Integer code = (Integer) responseHeaders.get(EzyActiveKeys.ERROR_CODE);
        if (status.equals(EzyActiveStatusCodes.NOT_FOUND))
            throw new NotFoundException(message);
        if (status.equals(EzyActiveStatusCodes.BAD_REQUEST))
            throw new BadRequestException(code, message);
        if (status.equals(EzyActiveStatusCodes.INTERNAL_SERVER_ERROR))
            throw new InternalServerErrorException(message);
    }
    
    protected void rawFire(
    		EzyActiveProperties requestProperties, byte[] requestMessage) {
    	try {
			client.doFire(requestProperties, requestMessage);
		} 
    	catch (Exception e) {
    		throw new InternalServerErrorException(e.getMessage(), e);
		}
    }
    
    protected EzyActiveMessage rawCall(
    		EzyActiveProperties requestProperties, byte[] requestMessage) {
    	try {
			EzyActiveMessage responseData = client.doCall(requestProperties, requestMessage);
			return responseData;
		} 
    	catch (TimeoutException e) {
			throw new EzyTimeoutException("call request: " + requestProperties.getType() + " timeout", e);
		}
    	catch (Exception e) {
    		throw new InternalServerErrorException(e.getMessage(), e);
		}
    }
    
    public static Builder builder() {
    		return new Builder();
    }
    
    public static class Builder implements EzyBuilder<EzyActiveRpcCaller> {
    	
    		protected EzyActiveRpcClient client;
    		protected EzyEntityCodec entityCodec;
    		
    		public Builder client(EzyActiveRpcClient client) {
    			this.client = client;
    			return this;
    		}
    		
    		public Builder entityCodec(EzyEntityCodec entityCodec) {
    			this.entityCodec = entityCodec;
    			return this;
    		}
    		
    		@Override
    		public EzyActiveRpcCaller build() {
    			return new EzyActiveRpcCaller(client, entityCodec);
    		}
    		
    }

}
