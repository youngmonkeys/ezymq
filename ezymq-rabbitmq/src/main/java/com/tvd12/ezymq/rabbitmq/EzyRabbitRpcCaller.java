package com.tvd12.ezymq.rabbitmq;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.RpcClient.Response;
import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezyfox.exception.BadRequestException;
import com.tvd12.ezyfox.exception.EzyTimeoutException;
import com.tvd12.ezyfox.exception.InternalServerErrorException;
import com.tvd12.ezyfox.exception.NotFoundException;
import com.tvd12.ezyfox.message.EzyMessageTypeFetcher;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezyfox.util.EzyLoggable;
import com.tvd12.ezymq.rabbitmq.constant.EzyRabbitKeys;
import com.tvd12.ezymq.rabbitmq.constant.EzyRabbitStatusCodes;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitRpcClient;

import java.util.Map;
import java.util.concurrent.TimeoutException;

public class EzyRabbitRpcCaller
    extends EzyLoggable implements EzyCloseable {

    protected final EzyRabbitRpcClient client;
    protected final EzyEntityCodec entityCodec;

    public EzyRabbitRpcCaller(
        EzyRabbitRpcClient client, EzyEntityCodec entityCodec) {
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
        processResponseHeaders(responseHeaders);
        byte[] responseBody = responseData.getBody();
        return entityCodec.deserialize(responseBody, returnType);
    }

    protected void processResponseHeaders(Map<String, Object> responseHeaders) {
        if (responseHeaders == null) {
            return;
        }
        Integer status = (Integer) responseHeaders.get(EzyRabbitKeys.STATUS);
        if (status == null) {
            return;
        }
        String message = responseHeaders.get(EzyRabbitKeys.MESSAGE).toString();
        Integer code = (Integer) responseHeaders.get(EzyRabbitKeys.ERROR_CODE);
        if (status.equals(EzyRabbitStatusCodes.NOT_FOUND)) {
            throw new NotFoundException(message);
        }
        if (status.equals(EzyRabbitStatusCodes.BAD_REQUEST)) {
            throw new BadRequestException(code, message);
        }
        if (status.equals(EzyRabbitStatusCodes.INTERNAL_SERVER_ERROR)) {
            throw new InternalServerErrorException(message);
        }
    }

    protected void rawFire(
        BasicProperties requestProperties, byte[] requestMessage) {
        try {
            client.doFire(requestProperties, requestMessage);
        } catch (Exception e) {
            throw new InternalServerErrorException(e.getMessage(), e);
        }
    }

    protected Response rawCall(
        BasicProperties requestProperties, byte[] requestMessage) {
        try {
            return client.doCall(requestProperties, requestMessage);
        } catch (TimeoutException e) {
            throw new EzyTimeoutException("call request: " + requestProperties.getType() + " timeout", e);
        } catch (Exception e) {
            throw new InternalServerErrorException(e.getMessage(), e);
        }
    }

    public static class Builder implements EzyBuilder<EzyRabbitRpcCaller> {

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
        public EzyRabbitRpcCaller build() {
            return new EzyRabbitRpcCaller(client, entityCodec);
        }
    }
}
