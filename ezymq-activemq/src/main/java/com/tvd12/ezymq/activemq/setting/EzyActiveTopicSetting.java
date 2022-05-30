package com.tvd12.ezymq.activemq.setting;

import lombok.Getter;

import javax.jms.Destination;
import javax.jms.Session;

import static com.tvd12.ezyfox.io.EzyStrings.isEmpty;

@Getter
public class EzyActiveTopicSetting extends EzyActiveEndpointSetting {

    protected final String topicName;
    protected final Destination topic;
    protected final boolean producerEnable;
    protected final boolean consumerEnable;
    protected final int serverThreadPoolSize;

    public EzyActiveTopicSetting(
        Session session,
        String topicName,
        Destination topic,
        boolean producerEnable,
        boolean consumerEnable,
        int serverThreadPoolSize
    ) {
        super(session);
        this.topic = topic;
        this.topicName = topicName;
        this.producerEnable = producerEnable;
        this.consumerEnable = consumerEnable;
        this.serverThreadPoolSize = serverThreadPoolSize;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends EzyActiveEndpointSetting.Builder<Builder> {

        protected String topicName;
        protected Destination topic;
        protected boolean producerEnable;
        protected boolean consumerEnable;
        protected int consumerThreadPoolSize = 1;
        protected EzyActiveSettings.Builder parent;

        public Builder() {
            this(null);
        }

        public Builder(EzyActiveSettings.Builder parent) {
            this.parent = parent;
        }

        public Builder topic(Destination topic) {
            this.topic = topic;
            return this;
        }

        public Builder topicName(String topicName) {
            if (isEmpty(this.topicName)) {
                this.topicName = topicName;
            }
            return this;
        }

        public Builder producerEnable(boolean producerEnable) {
            this.producerEnable = producerEnable;
            return this;
        }

        public Builder consumerEnable(boolean consumerEnable) {
            this.consumerEnable = consumerEnable;
            return this;
        }

        public Builder consumerThreadPoolSize(int consumerThreadPoolSize) {
            if (consumerThreadPoolSize > 0) {
                this.consumerThreadPoolSize = consumerThreadPoolSize;
            }
            return this;
        }

        public EzyActiveSettings.Builder parent() {
            return parent;
        }

        @Override
        public EzyActiveTopicSetting build() {
            return new EzyActiveTopicSetting(
                session,
                topicName,
                topic,
                producerEnable,
                consumerEnable,
                consumerThreadPoolSize
            );
        }
    }
}
