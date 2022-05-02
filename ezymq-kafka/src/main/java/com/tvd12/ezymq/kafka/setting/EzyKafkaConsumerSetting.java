package com.tvd12.ezymq.kafka.setting;

import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageHandler;
import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageHandlers;
import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageInterceptor;
import lombok.Getter;
import org.apache.kafka.clients.consumer.Consumer;

import java.util.Map;

@Getter
@SuppressWarnings("rawtypes")
public class EzyKafkaConsumerSetting extends EzyKafkaEndpointSetting {

    protected final long pollTimeOut;
    protected final Consumer consumer;
    protected final int threadPoolSize;
    protected final EzyKafkaMessageHandlers messageHandlers;
    protected final EzyKafkaMessageInterceptor messageInterceptor;

    public EzyKafkaConsumerSetting(
        String topic,
        Consumer consumer,
        long poolTimeOut,
        int threadPoolSize,
        EzyKafkaMessageHandlers requestHandlers,
        EzyKafkaMessageInterceptor actionInterceptor,
        Map<String, Object> properties) {
        super(topic, properties);
        this.consumer = consumer;
        this.pollTimeOut = poolTimeOut;
        this.threadPoolSize = threadPoolSize;
        this.messageHandlers = requestHandlers;
        this.messageInterceptor = actionInterceptor;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends EzyKafkaEndpointSetting.Builder<Builder> {

        protected Consumer consumer;
        protected int threadPoolSize = 3;
        protected long pollTimeOut = 100;
        protected EzyKafkaMessageHandlers messageHandlers;
        protected EzyKafkaMessageInterceptor messageInterceptor;
        protected EzyKafkaSettings.Builder parent;

        public Builder() {
            this(null);
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
            if (this.messageInterceptor == null) {
                this.messageInterceptor = messageInterceptor;
            }
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
                messageHandlers,
                messageInterceptor,
                properties);
        }
    }
}
