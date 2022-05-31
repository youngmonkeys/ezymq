package com.tvd12.ezymq.common.setting;

import com.tvd12.ezymq.common.EzyMQRpcProxyBuilder;
import com.tvd12.ezymq.common.annotation.EzyConsumerAnnotationProperties;
import com.tvd12.ezymq.common.handler.EzyMQMessageConsumer;
import com.tvd12.ezymq.common.handler.EzyMQRequestHandler;
import com.tvd12.ezymq.common.handler.EzyMQRequestInterceptor;
import lombok.Getter;

import java.util.*;

@Getter
@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class EzyMQRpcSettings extends EzyMQSettings {

    protected final Map<String, Class> requestTypeByCommand;
    protected final Map<String, Map<String, Class>> messageTypeMapByTopic;

    public EzyMQRpcSettings(
        Properties properties,
        Map<String, Class> requestTypeByCommand,
        Map<String, Map<String, Class>> messageTypeMapByTopic
    ) {
        super(properties);
        this.requestTypeByCommand = Collections.unmodifiableMap(
            requestTypeByCommand
        );
        this.messageTypeMapByTopic = Collections.unmodifiableMap(
            messageTypeMapByTopic
        );
    }

    @Override
    public Set<Class> getMessageTypes() {
        Set<Class> set = new HashSet<>(
            requestTypeByCommand.values()
        );
        for (String topic : messageTypeMapByTopic.keySet()) {
            set.addAll(
                messageTypeMapByTopic
                    .getOrDefault(topic, Collections.emptyMap())
                    .values()
            );
        }
        return set;
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
        protected final Map<String, Map<String, Class>> messageTypeMapByTopic =
            new HashMap<>();
        protected final Map<String, Map<String, List<EzyMQMessageConsumer>>> messageConsumersMapByTopic
            = new HashMap<>();

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

        public B mapTopicMessageType(
            String topic,
            String cmd,
            Class<?> messageType
        ) {
            this.messageTypeMapByTopic.computeIfAbsent(
                topic,
                k -> new HashMap<>()
            ).put(cmd, messageType);
            return (B) this;
        }

        public B mapTopicMessageTypes(
            String topic,
            Map<String, Class<?>> messageTypes
        ) {
            this.messageTypeMapByTopic.computeIfAbsent(
                topic,
                k -> new HashMap<>()
            ).putAll(messageTypes);
            return (B) this;
        }

        public B mapTopicMessageTypes(
            Map<String, Map<String, Class<?>>> messageTypesMap
        ) {
            for (String topic : messageTypesMap.keySet()) {
                mapTopicMessageTypes(
                    topic,
                    messageTypesMap.get(topic)
                );
            }
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
                this.mapRequestType(command, handler.getRequestType());
            }
            return (B) this;
        }

        protected abstract String getRequestCommand(Object handler);

        public B addMessageConsumer(
            String topic,
            String cmd,
            EzyMQMessageConsumer messageConsumer
        ) {
            this.messageConsumersMapByTopic.computeIfAbsent(
                topic,
                k -> new HashMap<>()
            ).computeIfAbsent(
                cmd, k -> new ArrayList<>()
            ).add(messageConsumer);

            this.mapTopicMessageType(
                topic,
                cmd,
                messageConsumer.getMessageType()
            );
            return (B) this;
        }

        public B addMessageConsumers(
            List<EzyMQMessageConsumer> messageConsumers
        ) {
            for (EzyMQMessageConsumer consumer : messageConsumers) {
                EzyConsumerAnnotationProperties props =
                    getConsumerAnnotationProperties(consumer);
                addMessageConsumer(
                    props.getTopic(),
                    props.getCommand(),
                    consumer
                );
            }
            return (B) this;
        }

        protected abstract EzyConsumerAnnotationProperties getConsumerAnnotationProperties(
            EzyMQMessageConsumer messageConsumer
        );

        @Override
        public abstract S build();
    }
}
