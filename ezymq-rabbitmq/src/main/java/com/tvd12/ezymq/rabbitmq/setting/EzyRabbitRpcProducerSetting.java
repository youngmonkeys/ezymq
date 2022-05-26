package com.tvd12.ezymq.rabbitmq.setting;

import com.rabbitmq.client.Channel;
import com.tvd12.ezymq.rabbitmq.factory.EzyRabbitCorrelationIdFactory;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitResponseConsumer;
import lombok.Getter;

@Getter
public class EzyRabbitRpcProducerSetting extends EzyRabbitEndpointSetting {

    protected final int capacity;
    protected final int defaultTimeout;
    protected final String requestQueueName;
    protected final String replyQueueName;
    protected final String replyRoutingKey;
    protected final String requestRoutingKey;
    protected final EzyRabbitCorrelationIdFactory correlationIdFactory;
    protected final EzyRabbitResponseConsumer unconsumedResponseConsumer;

    public EzyRabbitRpcProducerSetting(
        Channel channel,
        String exchange,
        int prefetchCount,
        String requestQueueName,
        String requestRoutingKey,
        String replyQueueName,
        String replyRoutingKey,
        int capacity,
        int defaultTimeout,
        EzyRabbitCorrelationIdFactory correlationIdFactory,
        EzyRabbitResponseConsumer unconsumedResponseConsumer
    ) {
        super(channel, exchange, prefetchCount);
        this.capacity = capacity;
        this.requestQueueName = requestQueueName;
        this.requestRoutingKey = requestRoutingKey;
        this.replyQueueName = replyQueueName;
        this.replyRoutingKey = replyRoutingKey;
        this.defaultTimeout = defaultTimeout;
        this.correlationIdFactory = correlationIdFactory;
        this.unconsumedResponseConsumer = unconsumedResponseConsumer;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends EzyRabbitEndpointSetting.Builder<Builder> {

        protected int capacity;
        protected int defaultTimeout;
        protected String requestQueueName;
        protected String requestRoutingKey;
        protected String replyQueueName;
        protected String replyRoutingKey;
        protected EzyRabbitCorrelationIdFactory correlationIdFactory;
        protected EzyRabbitResponseConsumer unconsumedResponseConsumer;
        protected EzyRabbitSettings.Builder parent;

        public Builder() {
            this(null);
        }

        public Builder(EzyRabbitSettings.Builder parent) {
            this.parent = parent;
            this.capacity = 10000;
        }

        public Builder capacity(int capacity) {
            this.capacity = capacity;
            return this;
        }

        public Builder defaultTimeout(int defaultTimeout) {
            this.defaultTimeout = defaultTimeout;
            return this;
        }

        public Builder requestQueueName(String requestQueueName) {
            this.requestQueueName = requestQueueName;
            return this;
        }

        public Builder requestRoutingKey(String requestRoutingKey) {
            this.requestRoutingKey = requestRoutingKey;
            return this;
        }

        public Builder replyQueueName(String replyQueueName) {
            this.replyQueueName = replyQueueName;
            return this;
        }

        public Builder replyRoutingKey(String replyRoutingKey) {
            this.replyRoutingKey = replyRoutingKey;
            return this;
        }

        public Builder correlationIdFactory(EzyRabbitCorrelationIdFactory correlationIdFactory) {
            this.correlationIdFactory = correlationIdFactory;
            return this;
        }

        public Builder unconsumedResponseConsumer(EzyRabbitResponseConsumer unconsumedResponseConsumer) {
            this.unconsumedResponseConsumer = unconsumedResponseConsumer;
            return this;
        }

        public EzyRabbitSettings.Builder parent() {
            return parent;
        }

        @Override
        public EzyRabbitRpcProducerSetting build() {
            return new EzyRabbitRpcProducerSetting(
                channel,
                exchange,
                prefetchCount,
                requestQueueName,
                requestRoutingKey,
                replyQueueName,
                replyRoutingKey,
                capacity,
                defaultTimeout,
                correlationIdFactory,
                unconsumedResponseConsumer
            );
        }
    }
}
