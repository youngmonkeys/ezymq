package com.tvd12.ezymq.rabbitmq.setting;

import com.rabbitmq.client.Channel;
import lombok.Getter;

@Getter
public class EzyRabbitTopicSetting extends EzyRabbitEndpointSetting {

    protected final boolean producerEnable;
    protected final String clientRoutingKey;
    protected final boolean consumerEnable;
    protected final String serverQueueName;

    public EzyRabbitTopicSetting(
        Channel channel,
        String exchange,
        int prefetchCount,
        boolean producerEnable,
        String clientRoutingKey,
        boolean consumerEnable,
        String serverQueueName
    ) {
        super(channel, exchange, prefetchCount);
        this.producerEnable = producerEnable;
        this.clientRoutingKey = clientRoutingKey;
        this.consumerEnable = consumerEnable;
        this.serverQueueName = serverQueueName;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends EzyRabbitEndpointSetting.Builder<Builder> {

        protected boolean producerEnable;
        protected String clientRoutingKey;
        protected boolean consumerEnable;
        protected String serverQueueName;
        protected EzyRabbitSettings.Builder parent;

        public Builder() {
            this(null);
        }

        public Builder(EzyRabbitSettings.Builder parent) {
            this.parent = parent;
        }

        public Builder producerEnable(boolean producerEnable) {
            this.producerEnable = producerEnable;
            return this;
        }

        public Builder clientRoutingKey(String clientRoutingKey) {
            this.clientRoutingKey = clientRoutingKey;
            return this;
        }

        public Builder consumerEnable(boolean consumerEnable) {
            this.consumerEnable = consumerEnable;
            return this;
        }

        public Builder serverQueueName(String serverQueueName) {
            this.serverQueueName = serverQueueName;
            return this;
        }

        public EzyRabbitSettings.Builder parent() {
            return parent;
        }

        @Override
        public EzyRabbitTopicSetting build() {
            return new EzyRabbitTopicSetting(
                channel,
                exchange,
                prefetchCount,
                producerEnable,
                clientRoutingKey,
                consumerEnable,
                serverQueueName
            );
        }
    }
}
