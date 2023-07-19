package com.tvd12.ezymq.mosquitto.concurrent;

import com.tvd12.ezyfox.concurrent.EzyThreadFactory;

public class EzyMqttThreadFactory extends EzyThreadFactory {

    protected EzyMqttThreadFactory(Builder builder) {
        super(builder);
    }

    public static EzyThreadFactory create(String poolName) {
        return builder().poolName(poolName).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends EzyThreadFactory.Builder {

        protected Builder() {
            super();
            this.prefix = "ezymq-mosquitto";
        }

        @Override
        public EzyThreadFactory build() {
            return new EzyMqttThreadFactory(this);
        }
    }
}
