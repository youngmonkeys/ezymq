package com.tvd12.ezymq.rabbitmq.setting;

import com.rabbitmq.client.Channel;
import com.tvd12.ezyfox.builder.EzyBuilder;
import lombok.Getter;

@Getter
public class EzyRabbitEndpointSetting {

    protected final Channel channel;
    protected final String exchange;
    protected final int prefetchCount;

    public EzyRabbitEndpointSetting(
        Channel channel,
        String exchange, int prefetchCount) {
        this.channel = channel;
        this.exchange = exchange;
        this.prefetchCount = prefetchCount;
    }

    @SuppressWarnings("unchecked")
    public abstract static class Builder<B extends Builder<B>>
        implements EzyBuilder<EzyRabbitEndpointSetting> {

        protected Channel channel;
        protected String exchange;
        protected int prefetchCount;

        public B channel(Channel channel) {
            this.channel = channel;
            return (B) this;
        }

        public B exchange(String exchange) {
            this.exchange = exchange;
            return (B) this;
        }

        public B prefetchCount(int prefetchCount) {
            this.prefetchCount = prefetchCount;
            return (B) this;
        }
    }
}
