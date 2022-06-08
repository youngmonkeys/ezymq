package com.tvd12.ezymq.rabbitmq;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.RpcClient.Response;
import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezyfox.exception.EzyTimeoutException;
import com.tvd12.ezyfox.exception.InternalServerErrorException;
import com.tvd12.ezyfox.message.EzyMessageTypeFetcher;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezyfox.util.EzyLoggable;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitRpcClient;

import java.util.Map;
import java.util.concurrent.TimeoutException;

import static com.tvd12.ezymq.common.util.EzyRpcExceptionTranslators.responseHeadersToException;

public class EzyRabbitRpcProducer
    extends EzyLoggable
    implements EzyCloseable {

    protected final EzyRabbitRpcClient client;
    protected final EzyEntityCodec entityCodec;

    public EzyRabbitRpcProducer(
        EzyRabbitRpcClient client,
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
        BasicProperties requestProperties = new BasicProperties.Builder()
            .type(cmd)
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
        BasicProperties requestProperties = new BasicProperties.Builder()
            .type(cmd)
            .build();
        byte[] requestMessage = entityCodec.serialize(data);
        Response responseData = rawCall(requestProperties, requestMessage);
        BasicProperties responseProperties = responseData.getProperties();
        Map<String, Object> responseHeaders = responseProperties.getHeaders();
        responseHeadersToException(responseHeaders);
        byte[] responseBody = responseData.getBody();
        return entityCodec.deserialize(responseBody, returnType);
    }

    protected void rawFire(
        BasicProperties requestProperties,
        byte[] requestMessage
    ) {
        try {
            client.doFire(requestProperties, requestMessage);
        } catch (Exception e) {
            throw new InternalServerErrorException(e.getMessage(), e);
        }
    }

    protected Response rawCall(
        BasicProperties requestProperties,
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

    public static class Builder implements EzyBuilder<EzyRabbitRpcProducer> {

        protected EzyRabbitRpcClient client;
        protected EzyEntityCodec entityCodec;

        public Builder client(EzyRabbitRpcClient client) {
            this.client = client;
            return this;
        }

        public Builder entityCodec(EzyEntityCodec entityCodec) {
            this.entityCodec = entityCodec;
            return this;
        }

        @Override
        public EzyRabbitRpcProducer build() {
            return new EzyRabbitRpcProducer(client, entityCodec);
        }
    }
}
