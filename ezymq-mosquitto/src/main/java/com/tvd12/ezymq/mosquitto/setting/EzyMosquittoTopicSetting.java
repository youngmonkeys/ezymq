package com.tvd12.ezymq.mosquitto.setting;

import java.util.List;
import java.util.Map;

import com.tvd12.ezymq.common.handler.EzyMQMessageConsumer;

import lombok.Getter;

@Getter
@SuppressWarnings("rawtypes")
public class EzyMosquittoTopicSetting extends EzyMosquittoEndpointSetting {

    protected final boolean producerEnable;
    protected final boolean consumerEnable;
    protected final Map<String, List<EzyMQMessageConsumer>> messageConsumersByTopic;

    public EzyMosquittoTopicSetting(
        String topic,
        boolean producerEnable,
        boolean consumerEnable,
        Map<String, List<EzyMQMessageConsumer>> messageConsumersByTopic
    ) {
        super(topic);
        this.producerEnable = producerEnable;
        this.consumerEnable = consumerEnable;
        this.messageConsumersByTopic = messageConsumersByTopic;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends EzyMosquittoEndpointSetting.Builder<Builder> {

        protected boolean producerEnable;
        protected boolean consumerEnable;
        protected final EzyMosquittoSettings.Builder parent;
        protected Map<String, List<EzyMQMessageConsumer>> messageConsumersByTopic;

        public Builder() {
            this(null);
        }

        public Builder(EzyMosquittoSettings.Builder parent) {
            this.parent = parent;
        }

        public Builder producerEnable(boolean producerEnable) {
            this.producerEnable = producerEnable;
            return this;
        }

        public Builder consumerEnable(boolean consumerEnable) {
            this.consumerEnable = consumerEnable;
            return this;
        }

        public Builder messageConsumersByTopic(
            Map<String, List<EzyMQMessageConsumer>> messageConsumersMap
        ) {
            this.messageConsumersByTopic = messageConsumersMap;
            return this;
        }

        public EzyMosquittoSettings.Builder parent() {
            return parent;
        }

        @Override
        public EzyMosquittoTopicSetting build() {
            return new EzyMosquittoTopicSetting(
                topic,
                producerEnable,
                consumerEnable,
                messageConsumersByTopic
            );
        }
    }
}
