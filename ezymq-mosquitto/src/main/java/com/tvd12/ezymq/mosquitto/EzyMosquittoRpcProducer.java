package com.tvd12.ezymq.mosquitto;

import static com.tvd12.ezymq.common.util.EzyRpcExceptionTranslators.responseHeadersToException;

import java.util.Map;
import java.util.concurrent.TimeoutException;

import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezyfox.exception.EzyTimeoutException;
import com.tvd12.ezyfox.exception.InternalServerErrorException;
import com.tvd12.ezyfox.message.EzyMessageTypeFetcher;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezyfox.util.EzyLoggable;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMosquittoMessage;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMosquittoRpcClient;
import com.tvd12.ezymq.mosquitto.util.EzyMosquittoProperties;

public class EzyMosquittoRpcProducer
    extends EzyLoggable
    implements EzyCloseable {

    protected final EzyMosquittoRpcClient client;
    protected final EzyEntityCodec entityCodec;

    public EzyMosquittoRpcProducer(
        EzyMosquittoRpcClient client,
        EzyEntityCodec entityCodec
    ) {
        this.client = client;
        this.entityCodec = entityCodec;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void close() {
        client.close();
    }

    public void fire(Object data) {
        if (!(data instanceof EzyMessageTypeFetcher)) {
            throw new IllegalArgumentException("data class must implement 'EzyMessageTypeFetcher'");
        }
        EzyMessageTypeFetcher mdata = (EzyMessageTypeFetcher) data;
        fire(mdata.getMessageType(), data);
    }

    public void fire(String cmd, Object data) {
        EzyMosquittoProperties requestProperties = new EzyMosquittoProperties.Builder()
            .messageType(cmd)
            .build();
        byte[] requestMessage = entityCodec.serialize(data);
        rawFire(requestProperties, requestMessage);
    }

    public <T> T call(Object data, Class<T> returnType) {
        if (!(data instanceof EzyMessageTypeFetcher)) {
            throw new IllegalArgumentException("data class must implement 'EzyMessageTypeFetcher'");
        }
        EzyMessageTypeFetcher mdata = (EzyMessageTypeFetcher) data;
        return call(mdata.getMessageType(), data, returnType);
    }

    public <T> T call(String cmd, Object data, Class<T> returnType) {
        EzyMosquittoProperties requestProperties = new EzyMosquittoProperties.Builder()
            .messageType(cmd)
            .build();
        byte[] requestMessage = entityCodec.serialize(data);
        EzyMosquittoMessage responseData = rawCall(requestProperties, requestMessage);
        EzyMosquittoProperties responseProperties = responseData.getProperties();
        Map<String, Object> responseHeaders = responseProperties.getHeaders();
        responseHeadersToException(responseHeaders);
        byte[] responseBody = responseData.getBody();
        return entityCodec.deserialize(responseBody, returnType);
    }

    protected void rawFire(
        EzyMosquittoProperties requestProperties,
        byte[] requestMessage
    ) {
        try {
            client.doFire(requestProperties, requestMessage);
        } catch (Exception e) {
            throw new InternalServerErrorException(e.getMessage(), e);
        }
    }

    protected EzyMosquittoMessage rawCall(
        EzyMosquittoProperties requestProperties,
        byte[] requestMessage
    ) {
        try {
            return client.doCall(requestProperties, requestMessage);
        } catch (TimeoutException e) {
            throw new EzyTimeoutException(
                "call request: " + requestProperties.getMessageType() + " timeout",
                e
            );
        } catch (Exception e) {
            throw new InternalServerErrorException(e.getMessage(), e);
        }
    }

    public static class Builder implements EzyBuilder<EzyMosquittoRpcProducer> {

        protected EzyMosquittoRpcClient client;
        protected EzyEntityCodec entityCodec;

        public Builder client(EzyMosquittoRpcClient client) {
            this.client = client;
            return this;
        }

        public Builder entityCodec(EzyEntityCodec entityCodec) {
            this.entityCodec = entityCodec;
            return this;
        }

        @Override
        public EzyMosquittoRpcProducer build() {
            return new EzyMosquittoRpcProducer(client, entityCodec);
        }
    }
}
