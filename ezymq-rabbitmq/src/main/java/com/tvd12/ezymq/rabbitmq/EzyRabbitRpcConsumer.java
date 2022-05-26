package com.tvd12.ezymq.rabbitmq;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.concurrent.EzyThreadList;
import com.tvd12.ezyfox.exception.BadRequestException;
import com.tvd12.ezyfox.exception.NotFoundException;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezyfox.util.EzyLoggable;
import com.tvd12.ezyfox.util.EzyStartable;
import com.tvd12.ezymq.rabbitmq.codec.EzyRabbitDataCodec;
import com.tvd12.ezymq.rabbitmq.concurrent.EzyRabbitThreadFactory;
import com.tvd12.ezymq.rabbitmq.constant.EzyRabbitErrorCodes;
import com.tvd12.ezymq.rabbitmq.constant.EzyRabbitKeys;
import com.tvd12.ezymq.rabbitmq.constant.EzyRabbitStatusCodes;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitRpcServer;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitActionInterceptor;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitRequestHandlers;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitRpcCallHandler;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadFactory;

public class EzyRabbitRpcConsumer
    extends EzyLoggable
    implements EzyRabbitRpcCallHandler, EzyStartable, EzyCloseable {

    protected final int threadPoolSize;
    protected final EzyRabbitRpcServer server;
    protected final EzyRabbitDataCodec dataCodec;
    protected final EzyRabbitRequestHandlers requestHandlers;
    protected EzyThreadList executorService;
    @Setter
    protected EzyRabbitActionInterceptor actionInterceptor;

    public EzyRabbitRpcConsumer(
        EzyRabbitRpcServer server,
        EzyRabbitDataCodec dataCodec,
        EzyRabbitRequestHandlers requestHandlers) {
        this(0, server, dataCodec, requestHandlers);
    }

    public EzyRabbitRpcConsumer(
        int threadPoolSize,
        EzyRabbitRpcServer server,
        EzyRabbitDataCodec dataCodec,
        EzyRabbitRequestHandlers requestHandlers) {
        this.server = server;
        this.server.setCallHandler(this);
        this.dataCodec = dataCodec;
        this.requestHandlers = requestHandlers;
        this.threadPoolSize = threadPoolSize;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void start() throws Exception {
        if (threadPoolSize > 0) {
            executorService = newExecutorService();
            executorService.execute();
        } else {
            server.start();
        }

    }

    protected void startLoop() {
        try {
            server.start();
        } catch (Exception e) {
            logger.error("rpc loop has exception", e);
        }
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

    @Override
    public void close() {
        server.close();
        executorService = null;
    }

    @Override
    public void handleFire(BasicProperties requestProperties, byte[] requestBody) {
        String cmd = requestProperties.getType();
        Object requestEntity = null;
        Object responseEntity;
        try {
            requestEntity = dataCodec.deserialize(cmd, requestBody);
            if (actionInterceptor != null) {
                actionInterceptor.intercept(cmd, requestEntity);
            }
            responseEntity = requestHandlers.handle(cmd, requestEntity);
            if (actionInterceptor != null) {
                actionInterceptor.intercept(cmd, requestEntity, responseEntity);
            }
        } catch (Exception e) {
            if (actionInterceptor != null) {
                actionInterceptor.intercept(cmd, requestEntity, e);
            }
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
            if (actionInterceptor != null) {
                actionInterceptor.intercept(cmd, requestEntity);
            }
            responseEntity = requestHandlers.handle(cmd, requestEntity);
            responseBytes = dataCodec.serialize(responseEntity);
            if (actionInterceptor != null) {
                actionInterceptor.intercept(cmd, requestEntity, responseEntity);
            }
        } catch (Exception e) {
            responseBytes = new byte[0];
            Map<String, Object> responseHeaders = new HashMap<>();
            if (e instanceof NotFoundException) {
                responseHeaders.put(EzyRabbitKeys.STATUS, EzyRabbitStatusCodes.NOT_FOUND);
            } else if (e instanceof BadRequestException) {
                BadRequestException badEx = (BadRequestException) e;
                responseHeaders.put(EzyRabbitKeys.STATUS, EzyRabbitStatusCodes.BAD_REQUEST);
                responseHeaders.put(EzyRabbitKeys.ERROR_CODE, badEx.getCode());
            } else if (e instanceof IllegalArgumentException) {
                responseHeaders.put(EzyRabbitKeys.STATUS, EzyRabbitStatusCodes.BAD_REQUEST);
                responseHeaders.put(EzyRabbitKeys.ERROR_CODE, EzyRabbitErrorCodes.INVALID_ARGUMENT);
            } else if (e instanceof UnsupportedOperationException) {
                responseHeaders.put(EzyRabbitKeys.STATUS, EzyRabbitStatusCodes.BAD_REQUEST);
                responseHeaders.put(EzyRabbitKeys.ERROR_CODE, EzyRabbitErrorCodes.UNSUPPORTED_OPERATION);
            } else {
                responseHeaders.put(EzyRabbitKeys.STATUS, EzyRabbitStatusCodes.INTERNAL_SERVER_ERROR);
            }

            String errorMessage = e.getMessage();
            if (errorMessage == null) {
                errorMessage = e.toString();
            }
            responseHeaders.put(EzyRabbitKeys.MESSAGE, errorMessage);
            replyPropertiesBuilder.headers(responseHeaders);

            if (actionInterceptor != null) {
                actionInterceptor.intercept(cmd, requestEntity, e);
            }
        }
        return responseBytes;
    }

    public static class Builder implements EzyBuilder<EzyRabbitRpcConsumer> {
        protected int threadPoolSize;
        protected EzyRabbitRpcServer server;
        protected EzyRabbitDataCodec dataCodec;
        protected EzyRabbitRequestHandlers requestHandlers;
        protected EzyRabbitActionInterceptor actionInterceptor;

        public Builder threadPoolSize(int threadPoolSize) {
            this.threadPoolSize = threadPoolSize;
            return this;
        }

        public Builder server(EzyRabbitRpcServer server) {
            this.server = server;
            return this;
        }

        public Builder dataCodec(EzyRabbitDataCodec dataCodec) {
            this.dataCodec = dataCodec;
            return this;
        }

        public Builder requestHandlers(EzyRabbitRequestHandlers requestHandlers) {
            this.requestHandlers = requestHandlers;
            return this;
        }

        public Builder actionInterceptor(EzyRabbitActionInterceptor actionInterceptor) {
            this.actionInterceptor = actionInterceptor;
            return this;
        }

        @Override
        public EzyRabbitRpcConsumer build() {
            EzyRabbitRpcConsumer product = new EzyRabbitRpcConsumer(
                threadPoolSize,
                server,
                dataCodec,
                requestHandlers);
            if (actionInterceptor != null) {
                product.setActionInterceptor(actionInterceptor);
            }
            return product;
        }
    }

}
