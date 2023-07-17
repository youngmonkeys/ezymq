package com.tvd12.ezymq.mosquitto;

import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.concurrent.EzyThreadList;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezyfox.util.EzyLoggable;
import com.tvd12.ezymq.common.codec.EzyMQDataCodec;
import com.tvd12.ezymq.mosquitto.handler.EzyMosquittoRequestHandlers;
import com.tvd12.ezymq.mosquitto.handler.EzyMosquittoRequestInterceptors;
import com.tvd12.ezymq.mosquitto.handler.EzyMosquittoRpcCallHandler;
import com.tvd12.ezymq.mosquitto.concurrent.EzyRabbitThreadFactory;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMosquittoRpcServer;
import com.tvd12.ezymq.mosquitto.util.EzyMosquittoProperties;

import java.util.concurrent.ThreadFactory;

import static com.tvd12.ezymq.common.util.EzyRpcExceptionTranslators.exceptionToResponseHeaders;

public class EzyMosquittoRpcConsumer
    extends EzyLoggable
    implements EzyMosquittoRpcCallHandler, EzyCloseable {

    protected final int threadPoolSize;
    protected final EzyMQDataCodec dataCodec;
    protected final EzyMosquittoRpcServer server;
    protected final EzyThreadList executorService;
    protected final EzyMosquittoRequestHandlers requestHandlers;
    protected final EzyMosquittoRequestInterceptors requestInterceptors;

    public EzyMosquittoRpcConsumer(
        int threadPoolSize,
        EzyMQDataCodec dataCodec,
        EzyMosquittoRpcServer server,
        EzyMosquittoRequestHandlers requestHandlers,
        EzyMosquittoRequestInterceptors requestInterceptors
    ) {
        this.server = server;
        this.server.setCallHandler(this);
        this.dataCodec = dataCodec;
        this.requestHandlers = requestHandlers;
        this.requestInterceptors = requestInterceptors;
        this.threadPoolSize = threadPoolSize;
        this.executorService = newExecutorService();
        this.executorService.execute();
    }

    protected EzyThreadList newExecutorService() {
        ThreadFactory threadFactory
            = EzyRabbitThreadFactory.create("rpc-handler");
        return new EzyThreadList(
            threadPoolSize,
            this::startLoop,
            threadFactory
        );
    }

    protected void startLoop() {
        try {
            server.start();
        } catch (Exception e) {
            logger.error("start consumer loop has exception", e);
        }
    }

    @Override
    public void close() {
        server.close();
    }

    @Override
    public void handleFire(
        EzyMosquittoProperties requestProperties,
        byte[] requestBody
    ) {
        String cmd = requestProperties.getMessageType();
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
        EzyMosquittoProperties requestProperties,
        byte[] requestBody,
        EzyMosquittoProperties.Builder replyPropertiesBuilder
    ) {
        String cmd = requestProperties.getMessageType();
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
            replyPropertiesBuilder.headers(exceptionToResponseHeaders(e));
            requestInterceptors.postHandle(cmd, requestEntity, e);
        }
        return responseBytes;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EzyBuilder<EzyMosquittoRpcConsumer> {
        protected int threadPoolSize = 1;
        protected EzyMosquittoRpcServer server;
        protected EzyMQDataCodec dataCodec;
        protected EzyMosquittoRequestHandlers requestHandlers;
        protected EzyMosquittoRequestInterceptors requestInterceptors;

        public Builder threadPoolSize(int threadPoolSize) {
            if (threadPoolSize > 0) {
                this.threadPoolSize = threadPoolSize;
            }
            return this;
        }

        public Builder server(EzyMosquittoRpcServer server) {
            this.server = server;
            return this;
        }

        public Builder dataCodec(EzyMQDataCodec dataCodec) {
            this.dataCodec = dataCodec;
            return this;
        }

        public Builder requestHandlers(
            EzyMosquittoRequestHandlers requestHandlers
        ) {
            this.requestHandlers = requestHandlers;
            return this;
        }

        public Builder requestInterceptors(
            EzyMosquittoRequestInterceptors requestInterceptors
        ) {
            this.requestInterceptors = requestInterceptors;
            return this;
        }

        @Override
        public EzyMosquittoRpcConsumer build() {
            return new EzyMosquittoRpcConsumer(
                threadPoolSize,
                dataCodec,
                server,
                requestHandlers,
                requestInterceptors
            );
        }
    }
}
