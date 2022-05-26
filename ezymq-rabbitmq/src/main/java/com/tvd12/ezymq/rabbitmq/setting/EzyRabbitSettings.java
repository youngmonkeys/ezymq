package com.tvd12.ezymq.rabbitmq.setting;

import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezymq.rabbitmq.EzyRabbitMQProxyBuilder;
import lombok.Getter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Getter
public class EzyRabbitSettings {

    protected final Map<String, Map<String, Object>> queueArguments;
    protected final Map<String, EzyRabbitTopicSetting> topicSettings;
    protected final Map<String, EzyRabbitRpcProducerSetting> rpcCallerSettings;
    protected final Map<String, EzyRabbitRpcConsumerSetting> rpcHandlerSettings;

    public EzyRabbitSettings(
        Map<String, Map<String, Object>> queueArguments,
        Map<String, EzyRabbitTopicSetting> topicSettings,
        Map<String, EzyRabbitRpcProducerSetting> rpcCallerSettings,
        Map<String, EzyRabbitRpcConsumerSetting> rpcHandlerSettings
    ) {
        this.queueArguments = Collections.unmodifiableMap(queueArguments);
        this.topicSettings = Collections.unmodifiableMap(topicSettings);
        this.rpcCallerSettings = Collections.unmodifiableMap(rpcCallerSettings);
        this.rpcHandlerSettings = Collections.unmodifiableMap(rpcHandlerSettings);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EzyBuilder<EzyRabbitSettings> {

        protected EzyRabbitMQProxyBuilder parent;
        protected Map<String, Map<String, Object>> queueArguments;
        protected Map<String, EzyRabbitTopicSetting> topicSettings;
        protected Map<String, EzyRabbitRpcProducerSetting> rpcCallerSettings;
        protected Map<String, EzyRabbitRpcConsumerSetting> rpcHandlerSettings;
        protected Map<String, EzyRabbitTopicSetting.Builder> topicSettingBuilders;
        protected Map<String, EzyRabbitRpcProducerSetting.Builder> rpcCallerSettingBuilders;
        protected Map<String, EzyRabbitRpcConsumerSetting.Builder> rpcHandlerSettingBuilders;

        public Builder() {
            this(null);
        }

        public Builder(EzyRabbitMQProxyBuilder parent) {
            this.parent = parent;
            this.topicSettings = new HashMap<>();
            this.queueArguments = new HashMap<>();
            this.rpcCallerSettings = new HashMap<>();
            this.rpcHandlerSettings = new HashMap<>();
            this.topicSettingBuilders = new HashMap<>();
            this.rpcCallerSettingBuilders = new HashMap<>();
            this.rpcHandlerSettingBuilders = new HashMap<>();
        }

        public Builder queueArgument(String queue, String key, Object value) {
            this.queueArguments.computeIfAbsent(queue, k -> new HashMap<>())
                .put(key, value);
            return this;
        }

        public Builder queueArguments(String queue, Map<String, Object> args) {
            this.queueArguments.computeIfAbsent(queue, k -> new HashMap<>())
                .putAll(args);
            return this;
        }

        public EzyRabbitTopicSetting.Builder topicSettingBuilder(String name) {
            return this.topicSettingBuilders.computeIfAbsent(
                name,
                k -> new EzyRabbitTopicSetting.Builder(this)
            );
        }

        public EzyRabbitRpcProducerSetting.Builder rpcCallerSettingBuilder(String name) {
            return this.rpcCallerSettingBuilders.computeIfAbsent(
                name,
                k -> new EzyRabbitRpcProducerSetting.Builder(this)
            );
        }

        public EzyRabbitRpcConsumerSetting.Builder rpcHandlerSettingBuilder(String name) {
            return this.rpcHandlerSettingBuilders.computeIfAbsent(
                name,
                k -> new EzyRabbitRpcConsumerSetting.Builder(this)
            );
        }

        public Builder addTopicSetting(String name, EzyRabbitTopicSetting setting) {
            this.topicSettings.put(name, setting);
            return this;
        }

        public Builder addRpcCallerSetting(String name, EzyRabbitRpcProducerSetting setting) {
            this.rpcCallerSettings.put(name, setting);
            return this;
        }

        public Builder addRpcHandlerSetting(String name, EzyRabbitRpcConsumerSetting setting) {
            this.rpcHandlerSettings.put(name, setting);
            return this;
        }


        public EzyRabbitMQProxyBuilder parent() {
            return parent;
        }

        @Override
        public EzyRabbitSettings build() {
            for (String name : topicSettingBuilders.keySet()) {
                EzyRabbitTopicSetting.Builder builder =
                    topicSettingBuilders.get(name);
                topicSettings.put(name, builder.build());
            }
            for (String name : rpcCallerSettingBuilders.keySet()) {
                EzyRabbitRpcProducerSetting.Builder builder =
                    rpcCallerSettingBuilders.get(name);
                rpcCallerSettings.put(name, builder.build());
            }
            for (String name : rpcHandlerSettingBuilders.keySet()) {
                EzyRabbitRpcConsumerSetting.Builder builder =
                    rpcHandlerSettingBuilders.get(name);
                rpcHandlerSettings.put(name, builder.build());
            }
            return new EzyRabbitSettings(
                queueArguments,
                topicSettings,
                rpcCallerSettings,
                rpcHandlerSettings
            );
        }
    }
}
