package com.tvd12.ezymq.rabbitmq.setting;

import com.rabbitmq.client.Channel;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitRequestHandler;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitRequestHandlers;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitRequestInterceptor;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitRequestInterceptors;
import lombok.Getter;

import java.util.Collection;
import java.util.Map;

@Getter
public class EzyRabbitRpcConsumerSetting extends EzyRabbitEndpointSetting {

    protected final int threadPoolSize;
    protected final String replyRoutingKey;
    protected final String requestQueueName;
    protected final EzyRabbitRequestHandlers requestHandlers;
    protected final EzyRabbitRequestInterceptors requestInterceptors;

    public EzyRabbitRpcConsumerSetting(
        Channel channel,
        String exchange,
        int prefetchCount,
        String replyRoutingKey,
        String requestQueueName,
        int threadPoolSize,
        EzyRabbitRequestHandlers requestHandlers,
        EzyRabbitRequestInterceptors requestInterceptors
    ) {
        super(channel, exchange, prefetchCount);
        this.replyRoutingKey = replyRoutingKey;
        this.requestQueueName = requestQueueName;
        this.threadPoolSize = threadPoolSize;
        this.requestHandlers = requestHandlers;
        this.requestInterceptors = requestInterceptors;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends EzyRabbitEndpointSetting.Builder<Builder> {

        protected int threadPoolSize = 1;
        protected String replyRoutingKey = "";
        protected String requestQueueName = null;
        protected EzyRabbitSettings.Builder parent;
        protected EzyRabbitRequestHandlers requestHandlers;
        protected EzyRabbitRequestInterceptors requestInterceptors;

        public Builder() {
            this(null);
        }

        public Builder(EzyRabbitSettings.Builder parent) {
            this.parent = parent;
            this.requestHandlers = new EzyRabbitRequestHandlers();
            this.requestInterceptors = new EzyRabbitRequestInterceptors();
        }

        public Builder threadPoolSize(int threadPoolSize) {
            this.threadPoolSize = threadPoolSize;
            return this;
        }

        public Builder requestQueueName(String requestQueueName) {
            this.requestQueueName = requestQueueName;
            return this;
        }

        public Builder replyRoutingKey(String replyRoutingKey) {
            this.replyRoutingKey = replyRoutingKey;
            return this;
        }

        public Builder addRequestInterceptor(
            EzyRabbitRequestInterceptor requestInterceptor
        ) {
            this.requestInterceptors.addInterceptor(requestInterceptor);
            return this;
        }

        public Builder addRequestInterceptors(
            Collection<EzyRabbitRequestInterceptor> requestInterceptors
        ) {
            this.requestInterceptors.addInterceptors(requestInterceptors);
            return this;
        }

        @SuppressWarnings("rawtypes")
        public Builder addRequestHandler(String cmd, EzyRabbitRequestHandler handler) {
            if (requestHandlers == null) {
                requestHandlers = new EzyRabbitRequestHandlers();
            }
            requestHandlers.addHandler(cmd, handler);
            return this;
        }

        @SuppressWarnings("rawtypes")
        public Builder addRequestHandlers(
            Map<String, EzyRabbitRequestHandler> handlers
        ) {
            for (String cmd : handlers.keySet()) {
                EzyRabbitRequestHandler handler = handlers.get(cmd);
                addRequestHandler(cmd, handler);
            }
            return this;
        }

        public EzyRabbitSettings.Builder parent() {
            return parent;
        }

        @Override
        public EzyRabbitRpcConsumerSetting build() {
            if (requestHandlers == null) {
                throw new NullPointerException("requestHandlers can not be null");
            }
            return new EzyRabbitRpcConsumerSetting(
                channel,
                exchange,
                prefetchCount,
                replyRoutingKey,
                requestQueueName,
                threadPoolSize,
                requestHandlers,
                requestInterceptors
            );
        }
    }
}
