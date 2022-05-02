package com.tvd12.ezymq.rabbitmq.concurrent;

import com.tvd12.ezyfox.concurrent.EzyThreadFactory;

public class EzyRabbitThreadFactory extends EzyThreadFactory {

    protected EzyRabbitThreadFactory(Builder builder) {
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
            this.prefix = "ezymq-rabbitmq";
        }

        @Override
        public EzyThreadFactory build() {
            return new EzyRabbitThreadFactory(this);
        }
    }
}
