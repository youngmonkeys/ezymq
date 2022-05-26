package com.tvd12.ezymq.activemq.setting;

import com.tvd12.ezymq.activemq.handler.EzyActiveActionInterceptor;
import com.tvd12.ezymq.activemq.handler.EzyActiveRequestHandler;
import com.tvd12.ezymq.activemq.handler.EzyActiveRequestHandlers;
import lombok.Getter;

import javax.jms.Destination;
import javax.jms.Session;
import java.util.Map;

@Getter
public class EzyActiveRpcConsumerSetting extends EzyActiveRpcEndpointSetting {

    protected final EzyActiveRequestHandlers requestHandlers;
    protected final EzyActiveActionInterceptor actionInterceptor;

    public EzyActiveRpcConsumerSetting(
        Session session,
        String requestQueueName,
        Destination requestQueue,
        String replyQueueName,
        Destination replyQueue,
        int threadPoolSize,
        EzyActiveRequestHandlers requestHandlers,
        EzyActiveActionInterceptor actionInterceptor
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
        this.actionInterceptor = actionInterceptor;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends EzyActiveRpcEndpointSetting.Builder<Builder> {

        protected EzyActiveRequestHandlers requestHandlers;
        protected EzyActiveActionInterceptor actionInterceptor;
        protected EzyActiveSettings.Builder parent;

        public Builder() {
            this(null);
        }

        public Builder(EzyActiveSettings.Builder parent) {
            this.parent = parent;
        }

        public Builder requestHandlers(EzyActiveRequestHandlers requestHandlers) {
            this.requestHandlers = requestHandlers;
            return this;
        }

        public Builder actionInterceptor(EzyActiveActionInterceptor actionInterceptor) {
            this.actionInterceptor = actionInterceptor;
            return this;
        }

        @SuppressWarnings("rawtypes")
        public Builder addRequestHandler(String cmd, EzyActiveRequestHandler handler) {
            if (requestHandlers == null) {
                requestHandlers = new EzyActiveRequestHandlers();
            }
            requestHandlers.addHandler(cmd, handler);
            return this;
        }

        @SuppressWarnings("rawtypes")
        public Builder addRequestHandler(Map<String, EzyActiveRequestHandler> handlers) {
            for (String cmd : handlers.keySet()) {
                EzyActiveRequestHandler handler = handlers.get(cmd);
                addRequestHandler(cmd, handler);
            }
            return this;
        }

        public EzyActiveSettings.Builder parent() {
            return parent;
        }

        @Override
        public EzyActiveRpcConsumerSetting build() {
            if (requestHandlers == null) {
                throw new NullPointerException("requestHandlers can not be null");
            }
            return new EzyActiveRpcConsumerSetting(
                session,
                requestQueueName,
                requestQueue,
                replyQueueName,
                replyQueue,
                threadPoolSize,
                requestHandlers,
                actionInterceptor
            );
        }
    }
}
