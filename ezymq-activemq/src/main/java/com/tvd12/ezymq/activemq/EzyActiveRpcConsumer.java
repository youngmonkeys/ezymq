package com.tvd12.ezymq.activemq;

import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezyfox.util.EzyLoggable;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveRpcServer;
import com.tvd12.ezymq.activemq.handler.EzyActiveRequestHandlers;
import com.tvd12.ezymq.activemq.handler.EzyActiveRequestInterceptors;
import com.tvd12.ezymq.activemq.handler.EzyActiveRpcCallHandler;
import com.tvd12.ezymq.activemq.util.EzyActiveProperties;
import com.tvd12.ezymq.common.codec.EzyMQDataCodec;

import static com.tvd12.ezymq.common.util.EzyRpcExceptionTranslators.exceptionToResponseHeaders;

public class EzyActiveRpcConsumer
    extends EzyLoggable
    implements EzyActiveRpcCallHandler, EzyCloseable {

    protected final EzyMQDataCodec dataCodec;
    protected final EzyActiveRpcServer server;
    protected final EzyActiveRequestHandlers requestHandlers;
    protected final EzyActiveRequestInterceptors requestInterceptors;

    public EzyActiveRpcConsumer(
        EzyMQDataCodec dataCodec,
        EzyActiveRpcServer server,
        EzyActiveRequestHandlers requestHandlers,
        EzyActiveRequestInterceptors requestInterceptors
    ) {
        this.dataCodec = dataCodec;
        this.requestHandlers = requestHandlers;
        this.requestInterceptors = requestInterceptors;
        this.server = server;
        this.server.setCallHandler(this);
        this.server.start();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void close() {
        server.close();
    }

    @Override
    public void handleFire(EzyActiveProperties requestProperties, byte[] requestBody) {
        String cmd = requestProperties.getType();
        Object requestEntity = null;
        Object responseEntity;
        try {
            requestEntity = dataCodec.deserialize(cmd, requestBody);
            requestInterceptors.preHandle(cmd, requestEntity);
            responseEntity = requestHandlers.handle(cmd, requestEntity);
            requestInterceptors.postHandle(cmd, requestEntity, responseEntity);
        } catch (Exception e) {
            requestInterceptors.postHandle(cmd, requestEntity, e);
        }
    }

    @Override
    public byte[] handleCall(
        EzyActiveProperties requestProperties,
        byte[] requestBody,
        EzyActiveProperties.Builder replyPropertiesBuilder
    ) {
        String cmd = requestProperties.getType();
        Object requestEntity = null;
        byte[] responseBytes;
        Object responseEntity;
        try {
            requestEntity = dataCodec.deserialize(cmd, requestBody);
            requestInterceptors.preHandle(cmd, requestEntity);
            responseEntity = requestHandlers.handle(cmd, requestEntity);
            responseBytes = dataCodec.serialize(responseEntity);
            requestInterceptors.postHandle(cmd, requestEntity, responseEntity);
        } catch (Throwable e) {
            responseBytes = new byte[0];
            replyPropertiesBuilder.addProperties(exceptionToResponseHeaders(e));
            requestInterceptors.postHandle(cmd, requestEntity, e);
        }
        return responseBytes;
    }

    public static class Builder implements EzyBuilder<EzyActiveRpcConsumer> {
        protected EzyMQDataCodec dataCodec;
        protected EzyActiveRpcServer server;
        protected EzyActiveRequestHandlers requestHandlers;
        protected EzyActiveRequestInterceptors requestInterceptors;

        public Builder server(EzyActiveRpcServer server) {
            this.server = server;
            return this;
        }

        public Builder dataCodec(EzyMQDataCodec dataCodec) {
            this.dataCodec = dataCodec;
            return this;
        }

        public Builder requestHandlers(
            EzyActiveRequestHandlers requestHandlers
        ) {
            this.requestHandlers = requestHandlers;
            return this;
        }

        public Builder requestInterceptors(
            EzyActiveRequestInterceptors requestInterceptors
        ) {
            this.requestInterceptors = requestInterceptors;
            return this;
        }

        @Override
        public EzyActiveRpcConsumer build() {
            if (requestInterceptors == null) {
                requestInterceptors = new EzyActiveRequestInterceptors();
            }
            return new EzyActiveRpcConsumer(
                dataCodec,
                server,
                requestHandlers,
                requestInterceptors
            );
        }
    }
}
