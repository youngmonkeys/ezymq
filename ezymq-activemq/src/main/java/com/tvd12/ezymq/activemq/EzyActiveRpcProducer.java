package com.tvd12.ezymq.activemq;

import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezyfox.exception.EzyTimeoutException;
import com.tvd12.ezyfox.exception.InternalServerErrorException;
import com.tvd12.ezyfox.message.EzyMessageTypeFetcher;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezyfox.util.EzyLoggable;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveMessage;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveRpcClient;
import com.tvd12.ezymq.activemq.util.EzyActiveProperties;

import java.util.concurrent.TimeoutException;

import static com.tvd12.ezymq.common.util.EzyRpcExceptionTranslators.responseHeadersToException;

public class EzyActiveRpcProducer
    extends EzyLoggable
    implements EzyCloseable {

    protected final EzyActiveRpcClient client;
    protected final EzyEntityCodec entityCodec;

    public EzyActiveRpcProducer(
        EzyActiveRpcClient client,
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
        EzyActiveProperties requestProperties = new EzyActiveProperties.Builder()
            .type(cmd)
            .build();
        byte[] requestMessage = entityCodec.serialize(data);
        rawFire(requestProperties, requestMessage);
    }

    protected void rawFire(
        EzyActiveProperties requestProperties,
        byte[] requestMessage
    ) {
        try {
            client.doFire(requestProperties, requestMessage);
        } catch (Exception e) {
            throw new InternalServerErrorException(e.getMessage(), e);
        }
    }

    public <T> T call(Object data, Class<T> returnType) {
        if (!(data instanceof EzyMessageTypeFetcher)) {
            throw new IllegalArgumentException("data class must implement 'EzyMessageTypeFetcher'");
        }
        EzyMessageTypeFetcher mdata = (EzyMessageTypeFetcher) data;
        return call(mdata.getMessageType(), data, returnType);
    }

    public <T> T call(String cmd, Object data, Class<T> returnType) {
        EzyActiveProperties requestProperties = new EzyActiveProperties.Builder()
            .type(cmd)
            .build();
        byte[] requestMessage = entityCodec.serialize(data);
        EzyActiveMessage responseData = rawCall(requestProperties, requestMessage);
        EzyActiveProperties responseProperties = responseData.getProperties();
        responseHeadersToException(responseProperties.getProperties());
        byte[] responseBody = responseData.getBody();
        return entityCodec.deserialize(responseBody, returnType);
    }

    protected EzyActiveMessage rawCall(
        EzyActiveProperties requestProperties,
        byte[] requestMessage
    ) {
        try {
            return client.doCall(requestProperties, requestMessage);
        } catch (TimeoutException e) {
            throw new EzyTimeoutException("call request: " + requestProperties.getType() + " timeout", e);
        } catch (Exception e) {
            throw new InternalServerErrorException(e.getMessage(), e);
        }
    }

    public static class Builder implements EzyBuilder<EzyActiveRpcProducer> {

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
        public EzyActiveRpcProducer build() {
            return new EzyActiveRpcProducer(client, entityCodec);
        }
    }
}
