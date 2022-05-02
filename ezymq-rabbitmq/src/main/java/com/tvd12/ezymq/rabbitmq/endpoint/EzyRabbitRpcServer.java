package com.tvd12.ezymq.rabbitmq.endpoint;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.ShutdownSignalException;
import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezyfox.util.EzyStartable;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitRpcCallHandler;
import lombok.Setter;

import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;

public class EzyRabbitRpcServer
    extends EzyRabbitEndpoint
    implements EzyStartable, EzyCloseable {

    protected final String exchange;
    protected final String replyRoutingKey;
    protected final String requestQueueName;
    protected final AtomicInteger startCount;
    protected final EzyRabbitBufferConsumer consumer;
    protected volatile boolean active;
    @Setter
    protected EzyRabbitRpcCallHandler callHandler;

    public EzyRabbitRpcServer(
        Channel channel,
        String exchange,
        String replyRoutingKey,
        String requestQueueName) throws Exception {
        super(channel, requestQueueName);
        this.exchange = exchange;
        this.replyRoutingKey = replyRoutingKey;
        this.requestQueueName = requestQueueName;
        this.startCount = new AtomicInteger();
        this.consumer = setupConsumer();
    }

    public static Builder builder() {
        return new Builder();
    }

    protected EzyRabbitBufferConsumer setupConsumer() throws Exception {
        EzyRabbitBufferConsumer consumer = new EzyRabbitBufferConsumer(channel);
        channel.basicConsume(requestQueueName, true, consumer);
        return consumer;
    }

    @Override
    public void start() throws Exception {
        this.active = true;
        this.startCount.incrementAndGet();
        while (active) {
            handleRequestOne();
        }
    }

    protected void handleRequestOne() {
        Delivery request = null;
        try {
            request = consumer.nextDelivery();
            if (request != null) {
                processRequest(request);
            } else {
                active = false;
            }
        } catch (Exception e) {
            if (e instanceof CancellationException) {
                this.active = false;
                logger.info("rpc server by request queue: {} has cancelled", requestQueueName, e);
            } else if (e instanceof ShutdownSignalException) {
                this.active = false;
                logger.info("rpc server by request queue: {} has shutted down", requestQueueName, e);
            } else {
                logger.warn("process request: {} of queue: {} error", request, requestQueueName, e);
            }
        }
    }

    public void processRequest(Delivery request)
        throws IOException {
        AMQP.BasicProperties requestProperties = request.getProperties();
        String correlationId = requestProperties.getCorrelationId();
        String responseRoutingKey = requestProperties.getReplyTo();
        if (responseRoutingKey == null) {
            responseRoutingKey = replyRoutingKey;
        }
        if (correlationId != null) {
            AMQP.BasicProperties.Builder replyPropertiesBuilder = new AMQP.BasicProperties.Builder();
            byte[] replyBody = handleCall(request, replyPropertiesBuilder);
            replyPropertiesBuilder.correlationId(correlationId);
            AMQP.BasicProperties replyProperties = replyPropertiesBuilder.build();
            channel.basicPublish(exchange, responseRoutingKey, replyProperties, replyBody);
        } else {
            handleFire(request);
        }
    }

    protected void handleFire(Delivery request) {
        callHandler.handleFire(request);
    }

    protected byte[] handleCall(
        Delivery request,
        BasicProperties.Builder replyPropertiesBuilder
    ) {
        return callHandler.handleCall(request, replyPropertiesBuilder);
    }

    @Override
    public void close() {
        this.active = false;
        this.callHandler = null;
        for (int i = 0; i < startCount.get(); ++i) {
            this.consumer.close();
        }
    }

    public static class Builder implements EzyBuilder<EzyRabbitRpcServer> {
        protected Channel channel = null;
        protected String exchange = "";
        protected String replyRoutingKey = "";
        protected String queueName = null;
        protected EzyRabbitRpcCallHandler callHandler = null;

        public Builder channel(Channel channel) {
            this.channel = channel;
            return this;
        }

        public Builder exchange(String exchange) {
            this.exchange = exchange;
            return this;
        }

        public Builder queueName(String queueName) {
            this.queueName = queueName;
            return this;
        }

        public Builder replyRoutingKey(String replyRoutingKey) {
            this.replyRoutingKey = replyRoutingKey;
            return this;
        }

        public Builder callHandler(EzyRabbitRpcCallHandler callHandler) {
            this.callHandler = callHandler;
            return this;
        }

        @Override
        public EzyRabbitRpcServer build() {
            try {
                EzyRabbitRpcServer server = new EzyRabbitRpcServer(
                    channel,
                    exchange,
                    replyRoutingKey,
                    queueName
                );
                server.setCallHandler(callHandler);
                return server;
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
