package com.tvd12.ezymq.common.setting;

import com.tvd12.ezymq.common.EzyMQRpcProxyBuilder;
import com.tvd12.ezymq.common.handler.EzyMQRequestHandler;
import com.tvd12.ezymq.common.handler.EzyMQRequestInterceptor;
import lombok.Getter;

import java.util.*;

@Getter
@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class EzyMQRpcSettings extends EzyMQSettings {

    protected final Map<String, Class> requestTypes;

    public EzyMQRpcSettings(
        Properties properties,
        Map<String, Class> requestTypes
    ) {
        super(properties);
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

        protected final List<RI> requestInterceptors =
            new ArrayList<>();
        protected final Map<String, Class> requestTypeByCommand =
            new HashMap<>();
        protected final Map<String, RH> requestHandlerByCommand =
            new HashMap<>();

        public Builder(EzyMQRpcProxyBuilder parent) {
            super(parent);
        }

        @Override
        public EzyMQRpcProxyBuilder parent() {
            return (EzyMQRpcProxyBuilder) super.parent();
        }

        public B mapRequestType(
            String command,
            Class requestType
        ) {
            this.requestTypeByCommand.put(command, requestType);
            return (B) this;
        }

        public B mapRequestTypes(
            Map<String, Class> requestTypes
        ) {
            this.requestTypeByCommand.putAll(requestTypes);
            return (B) this;
        }

        public B addRequestInterceptor(
            RI requestInterceptor
        ) {
            this.requestInterceptors.add(requestInterceptor);
            return (B) this;
        }

        public B addRequestInterceptors(
            Collection<RI> requestInterceptors
        ) {
            this.requestInterceptors.addAll(requestInterceptors);
            return (B) this;
        }

        public B addRequestHandlers(
            List<RH> requestHandlers
        ) {
            for (RH handler : requestHandlers) {
                String command = getRequestCommand(handler);
                this.requestHandlerByCommand.put(command, handler);
                this.requestTypeByCommand.put(command, handler.getRequestType());
            }
            return (B) this;
        }

        protected abstract String getRequestCommand(Object handler);

        @Override
        public abstract S build();
    }
}
