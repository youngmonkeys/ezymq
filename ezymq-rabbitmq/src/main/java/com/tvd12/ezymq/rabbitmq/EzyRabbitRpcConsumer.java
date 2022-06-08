package com.tvd12.ezymq.rabbitmq;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.concurrent.EzyThreadList;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezyfox.util.EzyLoggable;
import com.tvd12.ezymq.common.codec.EzyMQDataCodec;
import com.tvd12.ezymq.rabbitmq.concurrent.EzyRabbitThreadFactory;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitRpcServer;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitRequestHandlers;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitRequestInterceptors;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitRpcCallHandler;

import java.util.concurrent.ThreadFactory;

import static com.tvd12.ezymq.common.util.EzyRpcExceptionTranslators.exceptionToResponseHeaders;

public class EzyRabbitRpcConsumer
    extends EzyLoggable
    implements EzyRabbitRpcCallHandler, EzyCloseable {

    protected final int threadPoolSize;
    protected final EzyMQDataCodec dataCodec;
    protected final EzyRabbitRpcServer server;
    protected final EzyThreadList executorService;
    protected final EzyRabbitRequestHandlers requestHandlers;
    protected final EzyRabbitRequestInterceptors requestInterceptors;

    public EzyRabbitRpcConsumer(
        int threadPoolSize,
        EzyMQDataCodec dataCodec,
        EzyRabbitRpcServer server,
        EzyRabbitRequestHandlers requestHandlers,
        EzyRabbitRequestInterceptors requestInterceptors
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
        BasicProperties requestProperties,
        byte[] requestBody
    ) {
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
        BasicProperties requestProperties,
        byte[] requestBody,
        BasicProperties.Builder replyPropertiesBuilder) {
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
            replyPropertiesBuilder.headers(exceptionToResponseHeaders(e));
            requestInterceptors.postHandle(cmd, requestEntity, e);
        }
        return responseBytes;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EzyBuilder<EzyRabbitRpcConsumer> {
        protected int threadPoolSize = 1;
        protected EzyRabbitRpcServer server;
        protected EzyMQDataCodec dataCodec;
        protected EzyRabbitRequestHandlers requestHandlers;
        protected EzyRabbitRequestInterceptors requestInterceptors;

        public Builder threadPoolSize(int threadPoolSize) {
            if (threadPoolSize > 0) {
                this.threadPoolSize = threadPoolSize;
            }
            return this;
        }

        public Builder server(EzyRabbitRpcServer server) {
            this.server = server;
            return this;
        }

        public Builder dataCodec(EzyMQDataCodec dataCodec) {
            this.dataCodec = dataCodec;
            return this;
        }

        public Builder requestHandlers(
            EzyRabbitRequestHandlers requestHandlers
        ) {
            this.requestHandlers = requestHandlers;
            return this;
        }

        public Builder requestInterceptors(
            EzyRabbitRequestInterceptors requestInterceptors
        ) {
            this.requestInterceptors = requestInterceptors;
            return this;
        }

        @Override
        public EzyRabbitRpcConsumer build() {
            return new EzyRabbitRpcConsumer(
                threadPoolSize,
                dataCodec,
                server,
                requestHandlers,
                requestInterceptors
            );
        }
    }
}
