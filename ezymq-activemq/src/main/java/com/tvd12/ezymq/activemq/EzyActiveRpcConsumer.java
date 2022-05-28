package com.tvd12.ezymq.activemq;

import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.exception.BadRequestException;
import com.tvd12.ezyfox.exception.NotFoundException;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezyfox.util.EzyLoggable;
import com.tvd12.ezyfox.util.EzyStartable;
import com.tvd12.ezymq.activemq.codec.EzyActiveDataCodec;
import com.tvd12.ezymq.activemq.constant.EzyActiveErrorCodes;
import com.tvd12.ezymq.activemq.constant.EzyActiveKeys;
import com.tvd12.ezymq.activemq.constant.EzyActiveStatusCodes;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveRpcServer;
import com.tvd12.ezymq.activemq.handler.EzyActiveRequestHandlers;
import com.tvd12.ezymq.activemq.handler.EzyActiveRequestInterceptors;
import com.tvd12.ezymq.activemq.handler.EzyActiveRpcCallHandler;
import com.tvd12.ezymq.activemq.util.EzyActiveProperties;

import java.util.HashMap;
import java.util.Map;

public class EzyActiveRpcConsumer
    extends EzyLoggable
    implements EzyActiveRpcCallHandler, EzyStartable, EzyCloseable {

    protected final EzyActiveRpcServer server;
    protected final EzyActiveDataCodec dataCodec;
    protected final EzyActiveRequestHandlers requestHandlers;
    protected final EzyActiveRequestInterceptors requestInterceptors;

    public EzyActiveRpcConsumer(
        EzyActiveRpcServer server,
        EzyActiveDataCodec dataCodec,
        EzyActiveRequestHandlers requestHandlers,
        EzyActiveRequestInterceptors requestInterceptors
    ) {
        this.server = server;
        this.server.setCallHandler(this);
        this.dataCodec = dataCodec;
        this.requestHandlers = requestHandlers;
        this.requestInterceptors = requestInterceptors;
    }

    public static Builder builder() {
        return new Builder();
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
        } catch (Exception e) {
            responseBytes = new byte[0];
            Map<String, Object> responseHeaders = new HashMap<>();
            if (e instanceof NotFoundException) {
                responseHeaders.put(EzyActiveKeys.STATUS, EzyActiveStatusCodes.NOT_FOUND);
            } else if (e instanceof BadRequestException) {
                BadRequestException badEx = (BadRequestException) e;
                responseHeaders.put(EzyActiveKeys.STATUS, EzyActiveStatusCodes.BAD_REQUEST);
                responseHeaders.put(EzyActiveKeys.ERROR_CODE, badEx.getCode());
            } else if (e instanceof IllegalArgumentException) {
                responseHeaders.put(EzyActiveKeys.STATUS, EzyActiveStatusCodes.BAD_REQUEST);
                responseHeaders.put(EzyActiveKeys.ERROR_CODE, EzyActiveErrorCodes.INVALID_ARGUMENT);
            } else if (e instanceof UnsupportedOperationException) {
                responseHeaders.put(EzyActiveKeys.STATUS, EzyActiveStatusCodes.BAD_REQUEST);
                responseHeaders.put(EzyActiveKeys.ERROR_CODE, EzyActiveErrorCodes.UNSUPPORTED_OPERATION);
            } else {
                responseHeaders.put(EzyActiveKeys.STATUS, EzyActiveStatusCodes.INTERNAL_SERVER_ERROR);
            }

            String errorMessage = e.getMessage();
            if (errorMessage == null) {
                errorMessage = e.toString();
            }
            responseHeaders.put(EzyActiveKeys.MESSAGE, errorMessage);
            replyPropertiesBuilder.addProperties(responseHeaders);

            requestInterceptors.postHandle(cmd, requestEntity, e);
        }
        return responseBytes;
    }

    public static class Builder implements EzyBuilder<EzyActiveRpcConsumer> {
        protected int threadPoolSize;
        protected EzyActiveRpcServer server;
        protected EzyActiveDataCodec dataCodec;
        protected EzyActiveRequestHandlers requestHandlers;
        protected EzyActiveRequestInterceptors requestInterceptors;

        public Builder threadPoolSize(int threadPoolSize) {
            this.threadPoolSize = threadPoolSize;
            return this;
        }

        public Builder server(EzyActiveRpcServer server) {
            this.server = server;
            return this;
        }

        public Builder dataCodec(EzyActiveDataCodec dataCodec) {
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
                server,
                dataCodec,
                requestHandlers,
                requestInterceptors
            );
        }
    }
}
