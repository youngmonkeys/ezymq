package com.tvd12.ezymq.mosquitto.setting;

import static com.tvd12.properties.file.util.PropertiesUtil.getFirstPropertyKeys;
import static com.tvd12.properties.file.util.PropertiesUtil.getPropertiesByPrefix;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.tvd12.ezymq.common.annotation.EzyConsumerAnnotationProperties;
import com.tvd12.ezymq.common.handler.EzyMQMessageConsumer;
import com.tvd12.ezymq.common.setting.EzyMQRpcSettings;
import com.tvd12.ezymq.mosquitto.EzyMosquittoProxyBuilder;
import com.tvd12.ezymq.mosquitto.handler.EzyMosquittoRequestHandler;
import com.tvd12.ezymq.mosquitto.handler.EzyMosquittoRequestInterceptor;
import com.tvd12.ezymq.mosquitto.util.EzyMosquittoConsumerAnnotations;
import com.tvd12.ezymq.mosquitto.util.EzyMosquittoHandlerAnnotations;

import lombok.Getter;

@Getter
@SuppressWarnings("rawtypes")
public class EzyMosquittoSettings extends EzyMQRpcSettings {

    protected final Map<String, EzyMosquittoTopicSetting> topicSettings;
    protected final Map<String, EzyMosquittoRpcProducerSetting> rpcProducerSettings;
    protected final Map<String, EzyMosquittoRpcConsumerSetting> rpcConsumerSettings;

    public static final String KEY_SERVER_URI = "mosquitto.server_uri";
    public static final String KEY_CLIENT_PREFIX = "mosquitto.client_prefix";
    public static final String KEY_USERNAME = "mosquitto.username";
    public static final String KEY_PASSWORD = "mosquitto.password";
    public static final String KEY_MAX_CONNECTION_ATTEMPTS = "mosquitto.max_connection_attempts";
    public static final String KEY_CAPACITY = "capacity";
    public static final String KEY_CONSUMER = "consumer";
    public static final String KEY_CONSUMER_THREAD_POOL_SIZE = "consumer_thread_pool_size";
    public static final String KEY_CONSUMERS = "mosquitto.consumers";
    public static final String KEY_DEFAULT_TIMEOUT = "default_timeout";
    public static final String KEY_ENABLE = "enable";
    public static final String KEY_MAX_THREAD_POOL_SIZE = "mosquitto.max_thread_pool_size";
    public static final String KEY_PRODUCERS = "mosquitto.producers";
    public static final String KEY_PRODUCER = "producer";
    public static final String KEY_THREAD_POOL_SIZE = "thread_pool_size";
    public static final String KEY_TOPICS = "mosquitto.topics";

    public EzyMosquittoSettings(
        Properties properties,
        Map<String, Class> requestTypes,
        Map<String, Map<String, Class>> messageTypeMapByTopic,
        Map<String, EzyMosquittoTopicSetting> topicSettings,
        Map<String, EzyMosquittoRpcProducerSetting> rpcProducerSettings,
        Map<String, EzyMosquittoRpcConsumerSetting> rpcConsumerSettings
    ) {
        super(properties, requestTypes, messageTypeMapByTopic);
        this.topicSettings = Collections.unmodifiableMap(topicSettings);
        this.rpcProducerSettings = Collections.unmodifiableMap(rpcProducerSettings);
        this.rpcConsumerSettings = Collections.unmodifiableMap(rpcConsumerSettings);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends EzyMQRpcSettings.Builder<
        EzyMosquittoSettings,
        EzyMosquittoRequestInterceptor,
        EzyMosquittoRequestHandler,
        Builder
        > {

        protected Map<String, EzyMosquittoTopicSetting> topicSettings;
        protected Map<String, EzyMosquittoRpcProducerSetting> rpcProducerSettings;
        protected Map<String, EzyMosquittoRpcConsumerSetting> rpcConsumerSettings;
        protected Map<String, EzyMosquittoTopicSetting.Builder> topicSettingBuilders;
        protected Map<String, EzyMosquittoRpcProducerSetting.Builder> rpcProducerSettingBuilders;
        protected Map<String, EzyMosquittoRpcConsumerSetting.Builder> rpcConsumerSettingBuilders;

        public Builder() {
            this(null);
        }

        public Builder(EzyMosquittoProxyBuilder parent) {
            super(parent);
            this.topicSettings = new HashMap<>();
            this.rpcProducerSettings = new HashMap<>();
            this.rpcConsumerSettings = new HashMap<>();
            this.topicSettingBuilders = new HashMap<>();
            this.rpcProducerSettingBuilders = new HashMap<>();
            this.rpcConsumerSettingBuilders = new HashMap<>();
        }

        @Override
        public EzyMosquittoProxyBuilder parent() {
            return (EzyMosquittoProxyBuilder) super.parent();
        }

        public EzyMosquittoTopicSetting.Builder topicSettingBuilder(String name) {
            return this.topicSettingBuilders.computeIfAbsent(
                name,
                k -> new EzyMosquittoTopicSetting.Builder(this)
            );
        }

        public EzyMosquittoRpcProducerSetting.Builder rpcProducerSettingBuilder(String name) {
            return this.rpcProducerSettingBuilders.computeIfAbsent(
                name,
                k -> new EzyMosquittoRpcProducerSetting.Builder(this)
            );
        }

        public EzyMosquittoRpcConsumerSetting.Builder rpcConsumerSettingBuilder(String name) {
            return this.rpcConsumerSettingBuilders.computeIfAbsent(
                name,
                k -> new EzyMosquittoRpcConsumerSetting.Builder(this)
            );
        }

        public Builder addTopicSetting(String name, EzyMosquittoTopicSetting setting) {
            this.topicSettings.put(name, setting);
            return this;
        }

        public Builder addRpcProducerSetting(String name, EzyMosquittoRpcProducerSetting setting) {
            this.rpcProducerSettings.put(name, setting);
            return this;
        }

        public Builder addRpcConsumerSetting(String name, EzyMosquittoRpcConsumerSetting setting) {
            this.rpcConsumerSettings.put(name, setting);
            return this;
        }

        @Override
        protected String getRequestCommand(Object handler) {
            return EzyMosquittoHandlerAnnotations.getCommand(handler);
        }

        @Override
        protected EzyConsumerAnnotationProperties getConsumerAnnotationProperties(
            EzyMQMessageConsumer messageConsumer
        ) {
            return EzyMosquittoConsumerAnnotations.getProperties(messageConsumer);
        }

        @Override
        public EzyMosquittoSettings build() {
            buildTopicSettings();
            buildProducerSettings();
            buildConsumerSettings();
            return new EzyMosquittoSettings(
                properties,
                requestTypeByCommand,
                messageTypeMapByTopic,
                topicSettings,
                rpcProducerSettings,
                rpcConsumerSettings
            );
        }

        @SuppressWarnings("MethodLength")
        private void buildTopicSettings() {
            Properties topicsProperties =
                getPropertiesByPrefix(properties, KEY_TOPICS);
            Set<String> topicNames = new HashSet<>();
            topicNames.addAll(topicSettingBuilders.keySet());
            topicNames.addAll(getFirstPropertyKeys(topicsProperties));
            for (String name : topicNames) {
                Properties topicProperties = getPropertiesByPrefix(
                    topicsProperties,
                    name
                );
                EzyMosquittoTopicSetting.Builder builder = topicSettingBuilders
                    .computeIfAbsent(name, k ->
                        EzyMosquittoTopicSetting.builder()
                    );
                if (topicProperties.containsKey(KEY_PRODUCER)) {
                    Properties producerProperties = getPropertiesByPrefix(
                        topicProperties,
                        KEY_PRODUCER
                    );
                    builder
                        .producerEnable(
                            Boolean.parseBoolean(
                                producerProperties.getOrDefault(
                                    KEY_ENABLE,
                                    true
                                ).toString()
                            )
                        );
                }
                if (topicProperties.containsKey(KEY_CONSUMER)) {
                    Properties consumerProperties = getPropertiesByPrefix(
                        topicProperties,
                        KEY_CONSUMER
                    );
                    builder
                        .consumerEnable(
                            Boolean.parseBoolean(
                                consumerProperties.getOrDefault(
                                    KEY_ENABLE,
                                    true
                                ).toString()
                            )
                        )
                        .messageConsumersByTopic(
                            messageConsumersMapByTopic.getOrDefault(
                                name,
                                Collections.emptyMap()
                            )
                        );
                }
                topicSettings.put(name, builder.build());
            }
        }

        @SuppressWarnings("MethodLength")
        private void buildProducerSettings() {
            Properties producersProperties =
                getPropertiesByPrefix(properties, KEY_PRODUCERS);
            Set<String> producerNames = new HashSet<>();
            producerNames.addAll(rpcProducerSettingBuilders.keySet());
            producerNames.addAll(getFirstPropertyKeys(producersProperties));
            for (String name : producerNames) {
                Properties producerProperties = getPropertiesByPrefix(
                    producersProperties,
                    name
                );
                EzyMosquittoRpcProducerSetting producerSetting = rpcProducerSettingBuilders
                    .computeIfAbsent(name, k ->
                        EzyMosquittoRpcProducerSetting.builder()
                    )
                    .capacity(
                        Integer.parseInt(
                            producerProperties.getOrDefault(
                                KEY_CAPACITY,
                                0
                            ).toString()
                        )
                    )
                    .defaultTimeout(
                        Integer.parseInt(
                            producerProperties.getOrDefault(
                                KEY_DEFAULT_TIMEOUT,
                                0
                            ).toString()
                        )
                    )
                    .build();
                rpcProducerSettings.put(name, producerSetting);
            }
        }

        private void buildConsumerSettings() {
            Properties consumersProperties =
                getPropertiesByPrefix(properties, KEY_CONSUMERS);
            Set<String> consumerNames = new HashSet<>();
            consumerNames.addAll(rpcConsumerSettingBuilders.keySet());
            consumerNames.addAll(getFirstPropertyKeys(consumersProperties));
            for (String name : consumerNames) {
                Properties consumerProperties = getPropertiesByPrefix(
                    consumersProperties,
                    name
                );
                EzyMosquittoRpcConsumerSetting consumerSetting = rpcConsumerSettingBuilders
                    .computeIfAbsent(name, k ->
                        EzyMosquittoRpcConsumerSetting.builder()
                    )
                    .threadPoolSize(
                        Integer.parseInt(
                            consumerProperties.getOrDefault(
                                KEY_THREAD_POOL_SIZE,
                                0
                            ).toString()
                        )
                    )
                    .addRequestInterceptors(requestInterceptors)
                    .addRequestHandlers(requestHandlerByCommand)
                    .build();
                rpcConsumerSettings.put(name, consumerSetting);
            }
        }
    }
}
