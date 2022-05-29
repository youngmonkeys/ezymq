package com.tvd12.ezymq.activemq.setting;

import com.tvd12.ezymq.activemq.EzyActiveMQProxyBuilder;
import com.tvd12.ezymq.activemq.handler.EzyActiveRequestHandler;
import com.tvd12.ezymq.activemq.handler.EzyActiveRequestInterceptor;
import com.tvd12.ezymq.common.setting.EzyMQRpcSettings;
import lombok.Getter;

import java.util.*;

import static com.tvd12.ezymq.activemq.util.EzyActiveHandlerAnnotations.getCommand;

@Getter
@SuppressWarnings("rawtypes")
public class EzyActiveSettings extends EzyMQRpcSettings {

    protected final Map<String, EzyActiveTopicSetting> topicSettings;
    protected final Map<String, EzyActiveRpcProducerSetting> rpcProducerSettings;
    protected final Map<String, EzyActiveRpcConsumerSetting> rpcConsumerSettings;

    public EzyActiveSettings(
        Map<String, Class> requestTypes,
        Map<String, EzyActiveTopicSetting> topicSettings,
        Map<String, EzyActiveRpcProducerSetting> rpcProducerSettings,
        Map<String, EzyActiveRpcConsumerSetting> rpcConsumerSettings
    ) {
        super(requestTypes);
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

    public static class Builder extends EzyMQRpcSettings.Builder<
        EzyActiveSettings,
        EzyActiveRequestInterceptor,
        EzyActiveRequestHandler,
        Builder
        > {

        protected Map<String, EzyActiveTopicSetting> topicSettings;
        protected Map<String, EzyActiveRpcProducerSetting> rpcProducerSettings;
        protected Map<String, EzyActiveRpcConsumerSetting> rpcConsumerSettings;
        protected Map<String, EzyActiveTopicSetting.Builder> topicSettingBuilders;
        protected Map<String, EzyActiveRpcProducerSetting.Builder> rpcProducerSettingBuilders;
        protected Map<String, EzyActiveRpcConsumerSetting.Builder> rpcConsumerSettingBuilders;

        public Builder() {
            this(null);
        }

        public Builder(EzyActiveMQProxyBuilder parent) {
            super(parent);
            this.topicSettings = new HashMap<>();
            this.rpcProducerSettings = new HashMap<>();
            this.rpcConsumerSettings = new HashMap<>();
            this.topicSettingBuilders = new HashMap<>();
            this.rpcProducerSettingBuilders = new HashMap<>();
            this.rpcConsumerSettingBuilders = new HashMap<>();
        }

        @Override
        public EzyActiveMQProxyBuilder parent() {
            return (EzyActiveMQProxyBuilder) super.parent();
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

        @Override
        protected String getRequestCommand(Object handler) {
            return getCommand(handler);
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
