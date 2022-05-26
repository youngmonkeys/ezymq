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
    protected final Map<String, EzyRabbitRpcProducerSetting> rpcProducerSettings;
    protected final Map<String, EzyRabbitRpcConsumerSetting> rpcConsumerSettings;

    public EzyRabbitSettings(
        Map<String, Map<String, Object>> queueArguments,
        Map<String, EzyRabbitTopicSetting> topicSettings,
        Map<String, EzyRabbitRpcProducerSetting> rpcProducerSettings,
        Map<String, EzyRabbitRpcConsumerSetting> rpcConsumerSettings
    ) {
        this.queueArguments = Collections.unmodifiableMap(queueArguments);
        this.topicSettings = Collections.unmodifiableMap(topicSettings);
        this.rpcProducerSettings = Collections.unmodifiableMap(rpcProducerSettings);
        this.rpcConsumerSettings = Collections.unmodifiableMap(rpcConsumerSettings);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EzyBuilder<EzyRabbitSettings> {

        protected EzyRabbitMQProxyBuilder parent;
        protected Map<String, Map<String, Object>> queueArguments;
        protected Map<String, EzyRabbitTopicSetting> topicSettings;
        protected Map<String, EzyRabbitRpcProducerSetting> rpcProducerSettings;
        protected Map<String, EzyRabbitRpcConsumerSetting> rpcConsumerSettings;
        protected Map<String, EzyRabbitTopicSetting.Builder> topicSettingBuilders;
        protected Map<String, EzyRabbitRpcProducerSetting.Builder> rpcProducerSettingBuilders;
        protected Map<String, EzyRabbitRpcConsumerSetting.Builder> rpcConsumerSettingBuilders;

        public Builder() {
            this(null);
        }

        public Builder(EzyRabbitMQProxyBuilder parent) {
            this.parent = parent;
            this.topicSettings = new HashMap<>();
            this.queueArguments = new HashMap<>();
            this.rpcProducerSettings = new HashMap<>();
            this.rpcConsumerSettings = new HashMap<>();
            this.topicSettingBuilders = new HashMap<>();
            this.rpcProducerSettingBuilders = new HashMap<>();
            this.rpcConsumerSettingBuilders = new HashMap<>();
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

        public EzyRabbitRpcProducerSetting.Builder rpcProducerSettingBuilder(String name) {
            return this.rpcProducerSettingBuilders.computeIfAbsent(
                name,
                k -> new EzyRabbitRpcProducerSetting.Builder(this)
            );
        }

        public EzyRabbitRpcConsumerSetting.Builder rpcConsumerSettingBuilder(String name) {
            return this.rpcConsumerSettingBuilders.computeIfAbsent(
                name,
                k -> new EzyRabbitRpcConsumerSetting.Builder(this)
            );
        }

        public Builder addTopicSetting(String name, EzyRabbitTopicSetting setting) {
            this.topicSettings.put(name, setting);
            return this;
        }

        public Builder addRpcProducerSetting(String name, EzyRabbitRpcProducerSetting setting) {
            this.rpcProducerSettings.put(name, setting);
            return this;
        }

        public Builder addRpcConsumerSetting(String name, EzyRabbitRpcConsumerSetting setting) {
            this.rpcConsumerSettings.put(name, setting);
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
            for (String name : rpcProducerSettingBuilders.keySet()) {
                EzyRabbitRpcProducerSetting.Builder builder =
                    rpcProducerSettingBuilders.get(name);
                rpcProducerSettings.put(name, builder.build());
            }
            for (String name : rpcConsumerSettingBuilders.keySet()) {
                EzyRabbitRpcConsumerSetting.Builder builder =
                    rpcConsumerSettingBuilders.get(name);
                rpcConsumerSettings.put(name, builder.build());
            }
            return new EzyRabbitSettings(
                queueArguments,
                topicSettings,
                rpcProducerSettings,
                rpcConsumerSettings
            );
        }
    }
}
