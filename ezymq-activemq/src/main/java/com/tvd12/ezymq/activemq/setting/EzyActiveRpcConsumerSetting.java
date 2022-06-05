package com.tvd12.ezymq.activemq.setting;

import com.tvd12.ezymq.activemq.handler.EzyActiveRequestHandler;
import com.tvd12.ezymq.activemq.handler.EzyActiveRequestHandlers;
import com.tvd12.ezymq.activemq.handler.EzyActiveRequestInterceptor;
import com.tvd12.ezymq.activemq.handler.EzyActiveRequestInterceptors;
import lombok.Getter;

import javax.jms.Destination;
import javax.jms.Session;
import java.util.Collection;
import java.util.Map;

@Getter
public class EzyActiveRpcConsumerSetting extends EzyActiveRpcEndpointSetting {

    protected final EzyActiveRequestHandlers requestHandlers;
    protected final EzyActiveRequestInterceptors requestInterceptors;

    public EzyActiveRpcConsumerSetting(
        Session session,
        String requestQueueName,
        Destination requestQueue,
        String replyQueueName,
        Destination replyQueue,
        int threadPoolSize,
        EzyActiveRequestHandlers requestHandlers,
        EzyActiveRequestInterceptors requestInterceptors
    ) {
        super(
            session,
            requestQueueName,
            requestQueue,
            replyQueueName,
            replyQueue,
            threadPoolSize
        );
        this.requestHandlers = requestHandlers;
        this.requestInterceptors = requestInterceptors;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends EzyActiveRpcEndpointSetting.Builder<Builder> {

        protected final EzyActiveSettings.Builder parent;
        protected final EzyActiveRequestHandlers requestHandlers;
        protected final EzyActiveRequestInterceptors requestInterceptors;

        public Builder() {
            this(null);
        }

        public Builder(EzyActiveSettings.Builder parent) {
            this.parent = parent;
            this.requestHandlers = new EzyActiveRequestHandlers();
            this.requestInterceptors = new EzyActiveRequestInterceptors();
        }

        public Builder addRequestInterceptor(
            EzyActiveRequestInterceptor requestInterceptor
        ) {
            this.requestInterceptors.addInterceptor(requestInterceptor);
            return this;
        }

        public Builder addRequestInterceptors(
            Collection<EzyActiveRequestInterceptor> requestInterceptors
        ) {
            this.requestInterceptors.addInterceptors(requestInterceptors);
            return this;
        }

        @SuppressWarnings("rawtypes")
        public Builder addRequestHandler(
            String cmd,
            EzyActiveRequestHandler handler
        ) {
            requestHandlers.addHandler(cmd, handler);
            return this;
        }

        @SuppressWarnings("rawtypes")
        public Builder addRequestHandlers(
            Map<String, EzyActiveRequestHandler> handlers
        ) {
            requestHandlers.addHandlers(handlers);
            return this;
        }

        public EzyActiveSettings.Builder parent() {
            return parent;
        }

        @Override
        public EzyActiveRpcConsumerSetting build() {
            return new EzyActiveRpcConsumerSetting(
                session,
                requestQueueName,
                requestQueue,
                replyQueueName,
                replyQueue,
                threadPoolSize,
                requestHandlers,
                requestInterceptors
            );
        }
    }
}
