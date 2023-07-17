package com.tvd12.ezymq.mosquitto.setting;

import java.util.Collection;
import java.util.Map;

import com.tvd12.ezymq.mosquitto.handler.EzyMosquittoRequestHandler;
import com.tvd12.ezymq.mosquitto.handler.EzyMosquittoRequestHandlers;
import com.tvd12.ezymq.mosquitto.handler.EzyMosquittoRequestInterceptor;
import com.tvd12.ezymq.mosquitto.handler.EzyMosquittoRequestInterceptors;

import lombok.Getter;

@Getter
public class EzyMosquittoRpcConsumerSetting extends EzyMosquittoEndpointSetting {

    protected final int threadPoolSize;
    protected final EzyMosquittoRequestHandlers requestHandlers;
    protected final EzyMosquittoRequestInterceptors requestInterceptors;

    public EzyMosquittoRpcConsumerSetting(
        String name,
        int threadPoolSize,
        EzyMosquittoRequestHandlers requestHandlers,
        EzyMosquittoRequestInterceptors requestInterceptors
    ) {
        super(name);
        this.threadPoolSize = threadPoolSize;
        this.requestHandlers = requestHandlers;
        this.requestInterceptors = requestInterceptors;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends EzyMosquittoEndpointSetting.Builder<Builder> {

        protected int threadPoolSize = 1;
        protected String replyRoutingKey = "";
        protected String requestQueueName = null;
        protected final EzyMosquittoSettings.Builder parent;
        protected final EzyMosquittoRequestHandlers requestHandlers;
        protected final EzyMosquittoRequestInterceptors requestInterceptors;

        public Builder() {
            this(null);
        }

        public Builder(EzyMosquittoSettings.Builder parent) {
            this.parent = parent;
            this.requestHandlers = new EzyMosquittoRequestHandlers();
            this.requestInterceptors = new EzyMosquittoRequestInterceptors();
        }

        public Builder threadPoolSize(int threadPoolSize) {
            this.threadPoolSize = threadPoolSize;
            return this;
        }

        public Builder addRequestInterceptor(
            EzyMosquittoRequestInterceptor requestInterceptor
        ) {
            this.requestInterceptors.addInterceptor(requestInterceptor);
            return this;
        }

        public Builder addRequestInterceptors(
            Collection<EzyMosquittoRequestInterceptor> requestInterceptors
        ) {
            this.requestInterceptors.addInterceptors(requestInterceptors);
            return this;
        }

        @SuppressWarnings("rawtypes")
        public Builder addRequestHandler(String cmd, EzyMosquittoRequestHandler handler) {
            requestHandlers.addHandler(cmd, handler);
            return this;
        }

        @SuppressWarnings("rawtypes")
        public Builder addRequestHandlers(
            Map<String, EzyMosquittoRequestHandler> handlers
        ) {
            for (String cmd : handlers.keySet()) {
                EzyMosquittoRequestHandler handler = handlers.get(cmd);
                addRequestHandler(cmd, handler);
            }
            return this;
        }

        public EzyMosquittoSettings.Builder parent() {
            return parent;
        }

        @Override
        public EzyMosquittoRpcConsumerSetting build() {
            return new EzyMosquittoRpcConsumerSetting(
                topic,
                threadPoolSize,
                requestHandlers,
                requestInterceptors
            );
        }
    }
}
