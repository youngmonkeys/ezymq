package com.tvd12.ezymq.activemq.setting;

import com.tvd12.ezymq.activemq.factory.EzyActiveCorrelationIdFactory;
import com.tvd12.ezymq.activemq.handler.EzyActiveResponseConsumer;
import lombok.Getter;

import javax.jms.Destination;
import javax.jms.Session;

@Getter
public class EzyActiveRpcProducerSetting extends EzyActiveRpcEndpointSetting {

    protected final int capacity;
    protected final int defaultTimeout;
    protected final EzyActiveCorrelationIdFactory correlationIdFactory;
    protected final EzyActiveResponseConsumer unconsumedResponseConsumer;

    public EzyActiveRpcProducerSetting(
        Session session,
        String requestQueueName,
        Destination requestQueue,
        String replyQueueName,
        Destination replyQueue,
        int capacity,
        int threadPoolSize,
        int defaultTimeout,
        EzyActiveCorrelationIdFactory correlationIdFactory,
        EzyActiveResponseConsumer unconsumedResponseConsumer) {
        super(
            session,
            requestQueueName,
            requestQueue,
            replyQueueName,
            replyQueue,
            threadPoolSize
        );
        this.capacity = capacity;
        this.defaultTimeout = defaultTimeout;
        this.correlationIdFactory = correlationIdFactory;
        this.unconsumedResponseConsumer = unconsumedResponseConsumer;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends EzyActiveRpcEndpointSetting.Builder<Builder> {

        protected int capacity;
        protected int defaultTimeout;
        protected EzyActiveCorrelationIdFactory correlationIdFactory;
        protected EzyActiveResponseConsumer unconsumedResponseConsumer;
        protected EzyActiveSettings.Builder parent;

        public Builder() {
            this(null);
        }

        public Builder(EzyActiveSettings.Builder parent) {
            this.capacity = 10000;
            this.parent = parent;
        }

        public Builder capacity(int capacity) {
            if (capacity > 0) {
                this.capacity = capacity;
            }
            return this;
        }

        public Builder defaultTimeout(int defaultTimeout) {
            if (defaultTimeout > 0) {
                this.defaultTimeout = defaultTimeout;
            }
            return this;
        }

        public Builder correlationIdFactory(EzyActiveCorrelationIdFactory correlationIdFactory) {
            this.correlationIdFactory = correlationIdFactory;
            return this;
        }

        public Builder unconsumedResponseConsumer(EzyActiveResponseConsumer unconsumedResponseConsumer) {
            this.unconsumedResponseConsumer = unconsumedResponseConsumer;
            return this;
        }

        public EzyActiveSettings.Builder parent() {
            return parent;
        }

        @Override
        public EzyActiveRpcProducerSetting build() {
            return new EzyActiveRpcProducerSetting(
                session,
                requestQueueName,
                requestQueue,
                replyQueueName,
                replyQueue,
                capacity,
                threadPoolSize,
                defaultTimeout,
                correlationIdFactory,
                unconsumedResponseConsumer
            );
        }
    }
}
