package com.tvd12.ezymq.rabbitmq.setting;

import com.rabbitmq.client.Channel;
import com.tvd12.ezymq.common.handler.EzyMQMessageConsumer;
import lombok.Getter;

import java.util.List;
import java.util.Map;

@Getter
@SuppressWarnings("rawtypes")
public class EzyRabbitTopicSetting extends EzyRabbitEndpointSetting {

    protected final boolean producerEnable;
    protected final String producerRoutingKey;
    protected final boolean consumerEnable;
    protected final String consumerQueueName;
    protected final Map<String, List<EzyMQMessageConsumer>> messageConsumersByTopic;

    public EzyRabbitTopicSetting(
        Channel channel,
        String exchange,
        int prefetchCount,
        boolean producerEnable,
        String producerRoutingKey,
        boolean consumerEnable,
        String consumerQueueName,
        Map<String, List<EzyMQMessageConsumer>> messageConsumersByTopic
    ) {
        super(channel, exchange, prefetchCount);
        this.producerEnable = producerEnable;
        this.producerRoutingKey = producerRoutingKey;
        this.consumerEnable = consumerEnable;
        this.consumerQueueName = consumerQueueName;
        this.messageConsumersByTopic = messageConsumersByTopic;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends EzyRabbitEndpointSetting.Builder<Builder> {

        protected boolean producerEnable;
        protected String producerRoutingKey;
        protected boolean consumerEnable;
        protected String consumerQueueName;
        protected final EzyRabbitSettings.Builder parent;
        protected Map<String, List<EzyMQMessageConsumer>> messageConsumersByTopic;

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

        public Builder producerRoutingKey(String producerRoutingKey) {
            this.producerRoutingKey = producerRoutingKey;
            return this;
        }

        public Builder consumerEnable(boolean consumerEnable) {
            this.consumerEnable = consumerEnable;
            return this;
        }

        public Builder consumerQueueName(String consumerQueueName) {
            this.consumerQueueName = consumerQueueName;
            return this;
        }

        public Builder messageConsumersByTopic(
            Map<String, List<EzyMQMessageConsumer>> messageConsumersMap
        ) {
            this.messageConsumersByTopic = messageConsumersMap;
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
                producerRoutingKey,
                consumerEnable,
                consumerQueueName,
                messageConsumersByTopic
            );
        }
    }
}
