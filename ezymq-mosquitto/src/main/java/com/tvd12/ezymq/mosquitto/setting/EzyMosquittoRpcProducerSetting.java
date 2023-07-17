package com.tvd12.ezymq.mosquitto.setting;

import com.tvd12.ezymq.mosquitto.factory.EzyMosquittoMessageIdFactory;
import com.tvd12.ezymq.mosquitto.handler.EzyMosquittoResponseConsumer;

import lombok.Getter;

@Getter
public class EzyMosquittoRpcProducerSetting extends EzyMosquittoEndpointSetting {

    protected final int capacity;
    protected final int defaultTimeout;
    protected final EzyMosquittoMessageIdFactory messageIdFactory;
    protected final EzyMosquittoResponseConsumer unconsumedResponseConsumer;

    public EzyMosquittoRpcProducerSetting(
        String topic,
        int capacity,
        int defaultTimeout,
        EzyMosquittoMessageIdFactory messageIdFactory,
        EzyMosquittoResponseConsumer unconsumedResponseConsumer
    ) {
        super(topic);
        this.capacity = capacity;
        this.defaultTimeout = defaultTimeout;
        this.messageIdFactory = messageIdFactory;
        this.unconsumedResponseConsumer = unconsumedResponseConsumer;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends EzyMosquittoEndpointSetting.Builder<Builder> {

        protected int capacity;
        protected int defaultTimeout;
        protected EzyMosquittoMessageIdFactory messageIdFactory;
        protected EzyMosquittoResponseConsumer unconsumedResponseConsumer;
        protected EzyMosquittoSettings.Builder parent;

        public Builder() {
            this(null);
        }

        public Builder(EzyMosquittoSettings.Builder parent) {
            this.parent = parent;
            this.capacity = 10000;
        }

        public Builder capacity(int capacity) {
            if (capacity > 0) {
                this.capacity = capacity;
            }
            return this;
        }

        public Builder defaultTimeout(int defaultTimeout) {
            this.defaultTimeout = defaultTimeout;
            return this;
        }

        public Builder messageIdFactory(EzyMosquittoMessageIdFactory messageIdFactory) {
            this.messageIdFactory = messageIdFactory;
            return this;
        }

        public Builder unconsumedResponseConsumer(EzyMosquittoResponseConsumer unconsumedResponseConsumer) {
            this.unconsumedResponseConsumer = unconsumedResponseConsumer;
            return this;
        }

        public EzyMosquittoSettings.Builder parent() {
            return parent;
        }

        @Override
        public EzyMosquittoRpcProducerSetting build() {
            return new EzyMosquittoRpcProducerSetting(
                topic,
                capacity,
                defaultTimeout,
                messageIdFactory,
                unconsumedResponseConsumer
            );
        }
    }
}
