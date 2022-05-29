package com.tvd12.ezymq.rabbitmq.setting;

import com.tvd12.ezymq.common.setting.EzyMQRpcSettings;
import com.tvd12.ezymq.rabbitmq.EzyRabbitMQProxyBuilder;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitRequestHandler;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitRequestInterceptor;
import lombok.Getter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.tvd12.ezymq.rabbitmq.util.EzyRabbitHandlerAnnotations.getCommand;

@Getter
@SuppressWarnings("rawtypes")
public class EzyRabbitSettings extends EzyMQRpcSettings {

    protected final Map<String, Map<String, Object>> queueArguments;
    protected final Map<String, EzyRabbitTopicSetting> topicSettings;
    protected final Map<String, EzyRabbitRpcProducerSetting> rpcProducerSettings;
    protected final Map<String, EzyRabbitRpcConsumerSetting> rpcConsumerSettings;

    public static String URI = "rabbitmq.uri";
    public static String HOST = "rabbitmq.host";
    public static String PORT = "rabbitmq.port";
    public static String USERNAME = "rabbitmq.username";
    public static String PASSWORD = "rabbitmq.password";
    public static String VHOST = "rabbitmq.vhost";
    public static String MAX_CONNECTION_ATTEMPTS = "rabbitmq.requested_heart_beat";
    public static String REQUESTED_HEART_BEAT = "rabbitmq.max_connection_attempts";
    public static String SHARED_THREAD_POOL_SIZE = "rabbitmq.shared_thread_pool_size";

    public EzyRabbitSettings(
        Properties properties,
        Map<String, Class> requestTypes,
        Map<String, Map<String, Object>> queueArguments,
        Map<String, EzyRabbitTopicSetting> topicSettings,
        Map<String, EzyRabbitRpcProducerSetting> rpcProducerSettings,
        Map<String, EzyRabbitRpcConsumerSetting> rpcConsumerSettings
    ) {
        super(properties, requestTypes);
        this.queueArguments = Collections.unmodifiableMap(queueArguments);
        this.topicSettings = Collections.unmodifiableMap(topicSettings);
        this.rpcProducerSettings = Collections.unmodifiableMap(rpcProducerSettings);
        this.rpcConsumerSettings = Collections.unmodifiableMap(rpcConsumerSettings);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends EzyMQRpcSettings.Builder<
        EzyRabbitSettings,
        EzyRabbitRequestInterceptor,
        EzyRabbitRequestHandler,
        Builder
        > {

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
            super(parent);
            this.topicSettings = new HashMap<>();
            this.queueArguments = new HashMap<>();
            this.rpcProducerSettings = new HashMap<>();
            this.rpcConsumerSettings = new HashMap<>();
            this.topicSettingBuilders = new HashMap<>();
            this.rpcProducerSettingBuilders = new HashMap<>();
            this.rpcConsumerSettingBuilders = new HashMap<>();
        }

        @Override
        public EzyRabbitMQProxyBuilder parent() {
            return (EzyRabbitMQProxyBuilder) super.parent();
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

        @Override
        protected String getRequestCommand(Object handler) {
            return getCommand(handler);
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
                properties,
                requestTypeByCommand,
                queueArguments,
                topicSettings,
                rpcProducerSettings,
                rpcConsumerSettings
            );
        }
    }
}
