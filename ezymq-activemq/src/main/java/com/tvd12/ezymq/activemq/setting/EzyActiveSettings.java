package com.tvd12.ezymq.activemq.setting;

import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezymq.activemq.EzyActiveMQProxyBuilder;
import lombok.Getter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Getter
public class EzyActiveSettings {

    protected final Map<String, EzyActiveTopicSetting> topicSettings;
    protected final Map<String, EzyActiveRpcProducerSetting> rpcCallerSettings;
    protected final Map<String, EzyActiveRpcConsumerSetting> rpcHandlerSettings;

    public EzyActiveSettings(
        Map<String, EzyActiveTopicSetting> topicSettings,
        Map<String, EzyActiveRpcProducerSetting> rpcCallerSettings,
        Map<String, EzyActiveRpcConsumerSetting> rpcHandlerSettings
    ) {
        this.topicSettings = Collections.unmodifiableMap(topicSettings);
        this.rpcCallerSettings = Collections.unmodifiableMap(rpcCallerSettings);
        this.rpcHandlerSettings = Collections.unmodifiableMap(rpcHandlerSettings);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EzyBuilder<EzyActiveSettings> {

        protected EzyActiveMQProxyBuilder parent;
        protected Map<String, EzyActiveTopicSetting> topicSettings;
        protected Map<String, EzyActiveRpcProducerSetting> rpcCallerSettings;
        protected Map<String, EzyActiveRpcConsumerSetting> rpcHandlerSettings;
        protected Map<String, EzyActiveTopicSetting.Builder> topicSettingBuilders;
        protected Map<String, EzyActiveRpcProducerSetting.Builder> rpcCallerSettingBuilders;
        protected Map<String, EzyActiveRpcConsumerSetting.Builder> rpcHandlerSettingBuilders;

        public Builder() {
            this(null);
        }

        public Builder(EzyActiveMQProxyBuilder parent) {
            this.parent = parent;
            this.topicSettings = new HashMap<>();
            this.rpcCallerSettings = new HashMap<>();
            this.rpcHandlerSettings = new HashMap<>();
            this.topicSettingBuilders = new HashMap<>();
            this.rpcCallerSettingBuilders = new HashMap<>();
            this.rpcHandlerSettingBuilders = new HashMap<>();
        }

        public EzyActiveTopicSetting.Builder topicSettingBuilder(String name) {
            return topicSettingBuilders.computeIfAbsent(
                name, k -> new EzyActiveTopicSetting.Builder(this));
        }

        public EzyActiveRpcProducerSetting.Builder rpcCallerSettingBuilder(String name) {
            return rpcCallerSettingBuilders.computeIfAbsent(
                name, k -> new EzyActiveRpcProducerSetting.Builder(this));
        }

        public EzyActiveRpcConsumerSetting.Builder rpcHandlerSettingBuilder(String name) {
            return rpcHandlerSettingBuilders.computeIfAbsent(
                name, k -> new EzyActiveRpcConsumerSetting.Builder(this));
        }

        public Builder addTopicSetting(String name, EzyActiveTopicSetting setting) {
            this.topicSettings.put(name, setting);
            return this;
        }

        public Builder addRpcCallerSetting(String name, EzyActiveRpcProducerSetting setting) {
            this.rpcCallerSettings.put(name, setting);
            return this;
        }

        public Builder addRpcHandlerSetting(String name, EzyActiveRpcConsumerSetting setting) {
            this.rpcHandlerSettings.put(name, setting);
            return this;
        }


        public EzyActiveMQProxyBuilder parent() {
            return parent;
        }

        @Override
        public EzyActiveSettings build() {
            for (String name : topicSettingBuilders.keySet()) {
                EzyActiveTopicSetting.Builder builder = topicSettingBuilders.get(name);
                topicSettings.put(name, (EzyActiveTopicSetting) builder.build());
            }
            for (String name : rpcCallerSettingBuilders.keySet()) {
                EzyActiveRpcProducerSetting.Builder builder = rpcCallerSettingBuilders.get(name);
                rpcCallerSettings.put(name, builder.build());
            }
            for (String name : rpcHandlerSettingBuilders.keySet()) {
                EzyActiveRpcConsumerSetting.Builder builder = rpcHandlerSettingBuilders.get(name);
                rpcHandlerSettings.put(name, builder.build());
            }
            return new EzyActiveSettings(topicSettings, rpcCallerSettings, rpcHandlerSettings);
        }
    }
}
