package com.tvd12.ezymq.kafka.setting;

import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageHandler;
import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageHandlers;
import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageInterceptor;
import lombok.Getter;
import org.apache.kafka.clients.consumer.Consumer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@Getter
@SuppressWarnings("rawtypes")
public class EzyKafkaConsumerSetting extends EzyKafkaEndpointSetting {

    protected final long pollTimeOut;
    protected final Consumer consumer;
    protected final int threadPoolSize;
    protected final EzyKafkaMessageHandlers messageHandlers;
    protected final List<EzyKafkaMessageInterceptor> messageInterceptors;

    public EzyKafkaConsumerSetting(
        String topic,
        Consumer consumer,
        long poolTimeOut,
        int threadPoolSize,
        Map<String, Object> properties,
        EzyKafkaMessageHandlers requestHandlers,
        List<EzyKafkaMessageInterceptor> messageInterceptors
    ) {
        super(topic, properties);
        this.consumer = consumer;
        this.pollTimeOut = poolTimeOut;
        this.threadPoolSize = threadPoolSize;
        this.messageHandlers = requestHandlers;
        this.messageInterceptors = messageInterceptors;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends EzyKafkaEndpointSetting.Builder<Builder> {

        protected Consumer consumer;
        protected int threadPoolSize = 3;
        protected long pollTimeOut = 100;
        protected EzyKafkaSettings.Builder parent;
        protected EzyKafkaMessageHandlers messageHandlers;
        protected List<EzyKafkaMessageInterceptor> messageInterceptors;

        public Builder() {
            this(null);
            this.messageInterceptors = new ArrayList<>();
            this.messageHandlers = new EzyKafkaMessageHandlers();
        }

        public Builder(EzyKafkaSettings.Builder parent) {
            super();
            this.parent = parent;
        }

        public Builder pollTimeOut(long pollTimeOut) {
            this.pollTimeOut = pollTimeOut;
            return this;
        }

        public Builder threadPoolSize(int threadPoolSize) {
            this.threadPoolSize = threadPoolSize;
            return this;
        }

        public Builder consumer(Consumer consumer) {
            this.consumer = consumer;
            return this;
        }

        public Builder messageHandlers(EzyKafkaMessageHandlers messageHandlers) {
            this.messageHandlers = messageHandlers;
            return this;
        }

        public Builder messageInterceptor(EzyKafkaMessageInterceptor messageInterceptor) {
            this.messageInterceptors.add(messageInterceptor);
            return this;
        }

        public Builder messageInterceptors(Collection<EzyKafkaMessageInterceptor> messageInterceptors) {
            this.messageInterceptors.addAll(messageInterceptors);
            return this;
        }

        public Builder addMessageHandler(String cmd, EzyKafkaMessageHandler handler) {
            this.messageHandlers.addHandler(cmd, handler);
            return this;
        }

        public Builder addMessageHandlers(Map<String, EzyKafkaMessageHandler> handlers) {
            if (handlers != null) {
                for (String cmd : handlers.keySet()) {
                    EzyKafkaMessageHandler handler = handlers.get(cmd);
                    addMessageHandler(cmd, handler);
                }
            }
            return this;
        }

        public EzyKafkaSettings.Builder parent() {
            return parent;
        }

        @Override
        public EzyKafkaConsumerSetting build() {
            if (topic == null) {
                throw new NullPointerException("topic can not be null");
            }
            return new EzyKafkaConsumerSetting(
                topic,
                consumer,
                pollTimeOut,
                threadPoolSize,
                properties,
                messageHandlers,
                messageInterceptors
            );
        }
    }
}
