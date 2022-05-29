package com.tvd12.ezymq.common.setting;

import com.tvd12.ezymq.common.EzyMQRpcProxyBuilder;
import com.tvd12.ezymq.common.handler.EzyMQRequestHandler;
import com.tvd12.ezymq.common.handler.EzyMQRequestInterceptor;
import lombok.Getter;

import java.util.*;

@Getter
@SuppressWarnings("rawtypes")
public class EzyMQRpcSettings extends EzyMQSettings {

    protected final Map<String, Class> requestTypes;

    public EzyMQRpcSettings(Map<String, Class> requestTypes) {
        this.requestTypes = Collections.unmodifiableMap(requestTypes);
    }

    @Override
    public List<Class> getMessageTypeList() {
        return new ArrayList<>(requestTypes.values());
    }

    public abstract static class Builder<
        S extends EzyMQRpcSettings,
        RI extends EzyMQRequestInterceptor,
        RH extends EzyMQRequestHandler,
        B extends Builder<S, RI, RH, B>
        >
        extends EzyMQSettings.Builder<S, B> {

        protected final Map<String, Class> requestTypes =
            new HashMap<>();
        protected final List<RI> requestInterceptors =
            new ArrayList<>();
        protected final Map<String, RH> requestHandlerByCommand =
            new HashMap<>();

        public Builder(EzyMQRpcProxyBuilder parent) {
            super(parent);
        }

        @Override
        public EzyMQRpcProxyBuilder parent() {
            return (EzyMQRpcProxyBuilder) super.parent();
        }

        public Builder<S, RI, RH, B> mapRequestType(
            String command,
            Class requestType
        ) {
            this.requestTypes.put(command, requestType);
            return this;
        }

        public Builder<S, RI, RH, B> mapRequestTypes(
            Map<String, Class> requestTypes
        ) {
            this.requestTypes.putAll(requestTypes);
            return this;
        }

        public Builder<S, RI, RH, B> addRequestInterceptor(
            RI requestInterceptor
        ) {
            this.requestInterceptors.add(requestInterceptor);
            return this;
        }

        public Builder<S, RI, RH, B> addRequestInterceptors(
            Collection<RI> requestInterceptors
        ) {
            this.requestInterceptors.addAll(requestInterceptors);
            return this;
        }

        public Builder<S, RI, RH, B> addRequestHandlers(
            List<RH> requestHandlers
        ) {
            for (RH handler : requestHandlers) {
                String command = getRequestCommand(handler);
                this.requestHandlerByCommand.put(command, handler);
                mapRequestType(command, handler.getRequestType());
            }
            return this;
        }

        protected abstract String getRequestCommand(Object handler);

        @Override
        public abstract S build();
    }
}
