package com.tvd12.ezymq.activemq.setting;

import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezymq.activemq.EzyActiveMQProxyBuilder;
import com.tvd12.ezymq.activemq.handler.EzyActiveRequestHandler;
import com.tvd12.ezymq.activemq.handler.EzyActiveRequestInterceptor;
import lombok.Getter;

import java.util.*;

import static com.tvd12.ezymq.activemq.util.EzyActiveHandlerAnnotations.getCommand;

@Getter
@SuppressWarnings("rawtypes")
public class EzyActiveSettings {

    protected final Map<String, Class> requestTypes;
    protected final Map<String, EzyActiveTopicSetting> topicSettings;
    protected final Map<String, EzyActiveRpcProducerSetting> rpcProducerSettings;
    protected final Map<String, EzyActiveRpcConsumerSetting> rpcConsumerSettings;

    public EzyActiveSettings(
        Map<String, Class> requestTypes,
        Map<String, EzyActiveTopicSetting> topicSettings,
        Map<String, EzyActiveRpcProducerSetting> rpcProducerSettings,
        Map<String, EzyActiveRpcConsumerSetting> rpcConsumerSettings
    ) {
        this.requestTypes = Collections.unmodifiableMap(requestTypes);
        this.topicSettings = Collections.unmodifiableMap(topicSettings);
        this.rpcProducerSettings = Collections.unmodifiableMap(rpcProducerSettings);
        this.rpcConsumerSettings = Collections.unmodifiableMap(rpcConsumerSettings);
    }

    public static Builder builder() {
        return new Builder();
    }

    public List<Class> getMessageTypeList() {
        return new ArrayList<>(requestTypes.values());
    }

    public static class Builder implements EzyBuilder<EzyActiveSettings> {

        protected EzyActiveMQProxyBuilder parent;
        protected Map<String, Class> requestTypes;
        protected Map<String, EzyActiveTopicSetting> topicSettings;
        protected Map<String, EzyActiveRpcProducerSetting> rpcProducerSettings;
        protected Map<String, EzyActiveRpcConsumerSetting> rpcConsumerSettings;
        protected Map<String, EzyActiveTopicSetting.Builder> topicSettingBuilders;
        protected List<EzyActiveRequestInterceptor> requestInterceptors;
        protected Map<String, EzyActiveRequestHandler> requestHandlerByCommand;
        protected Map<String, EzyActiveRpcProducerSetting.Builder> rpcProducerSettingBuilders;
        protected Map<String, EzyActiveRpcConsumerSetting.Builder> rpcConsumerSettingBuilders;

        public Builder() {
            this(null);
        }

        public Builder(EzyActiveMQProxyBuilder parent) {
            this.parent = parent;
            this.requestTypes = new HashMap<>();
            this.topicSettings = new HashMap<>();
            this.rpcProducerSettings = new HashMap<>();
            this.rpcConsumerSettings = new HashMap<>();
            this.requestInterceptors = new ArrayList<>();
            this.requestHandlerByCommand = new HashMap<>();
            this.topicSettingBuilders = new HashMap<>();
            this.rpcProducerSettingBuilders = new HashMap<>();
            this.rpcConsumerSettingBuilders = new HashMap<>();
        }

        public EzyActiveTopicSetting.Builder topicSettingBuilder(String name) {
            return topicSettingBuilders.computeIfAbsent(name,
                k -> new EzyActiveTopicSetting.Builder(this)
            );
        }

        public EzyActiveRpcProducerSetting.Builder rpcProducerSettingBuilder(
            String name
        ) {
            return rpcProducerSettingBuilders.computeIfAbsent(name,
                k -> new EzyActiveRpcProducerSetting.Builder(this)
            );
        }

        public EzyActiveRpcConsumerSetting.Builder rpcConsumerSettingBuilder(
            String name
        ) {
            return rpcConsumerSettingBuilders.computeIfAbsent(name,
                k -> new EzyActiveRpcConsumerSetting.Builder(this)
            );
        }

        public Builder addTopicSetting(
            String name,
            EzyActiveTopicSetting setting
        ) {
            this.topicSettings.put(name, setting);
            return this;
        }

        public Builder addRpcProducerSetting(
            String name,
            EzyActiveRpcProducerSetting setting
        ) {
            this.rpcProducerSettings.put(name, setting);
            return this;
        }

        public Builder addRpcConsumerSetting(
            String name,
            EzyActiveRpcConsumerSetting setting
        ) {
            this.rpcConsumerSettings.put(name, setting);
            return this;
        }

        public Builder mapRequestType(String command, Class requestType) {
            this.requestTypes.put(command, requestType);
            return this;
        }

        public Builder mapRequestTypes(Map<String, Class> requestTypes) {
            this.requestTypes.putAll(requestTypes);
            return this;
        }

        public Builder addRequestInterceptor(
            EzyActiveRequestInterceptor requestInterceptor
        ) {
            this.requestInterceptors.add(requestInterceptor);
            return this;
        }

        public Builder addRequestInterceptors(
            Collection<EzyActiveRequestInterceptor> requestInterceptors
        ) {
            this.requestInterceptors.addAll(requestInterceptors);
            return this;
        }

        public Builder addRequestHandlers(
            List<EzyActiveRequestHandler> requestHandlers
        ) {
            for (EzyActiveRequestHandler handler : requestHandlers) {
                String command = getCommand(handler);
                this.requestHandlerByCommand.put(command, handler);
                mapRequestType(command, handler.getRequestType());
            }
            return this;
        }

        public EzyActiveMQProxyBuilder parent() {
            return parent;
        }

        @Override
        public EzyActiveSettings build() {
            for (String name : topicSettingBuilders.keySet()) {
                EzyActiveTopicSetting.Builder builder =
                    topicSettingBuilders.get(name);
                topicSettings.put(name, (EzyActiveTopicSetting) builder.build());
            }
            for (String name : rpcProducerSettingBuilders.keySet()) {
                EzyActiveRpcProducerSetting.Builder builder =
                    rpcProducerSettingBuilders.get(name);
                rpcProducerSettings.put(name, builder.build());
            }
            for (String name : rpcConsumerSettingBuilders.keySet()) {
                EzyActiveRpcConsumerSetting.Builder builder =
                    rpcConsumerSettingBuilders.get(name)
                        .addRequestInterceptors(requestInterceptors)
                        .addRequestHandlers(requestHandlerByCommand);
                rpcConsumerSettings.put(name, builder.build());
            }
            return new EzyActiveSettings(
                requestTypes,
                topicSettings,
                rpcProducerSettings,
                rpcConsumerSettings
            );
        }
    }
}
