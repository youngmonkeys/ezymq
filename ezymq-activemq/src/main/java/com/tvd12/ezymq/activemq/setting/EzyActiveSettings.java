package com.tvd12.ezymq.activemq.setting;

import com.tvd12.ezymq.activemq.EzyActiveMQProxyBuilder;
import com.tvd12.ezymq.activemq.handler.EzyActiveRequestHandler;
import com.tvd12.ezymq.activemq.handler.EzyActiveRequestInterceptor;
import com.tvd12.ezymq.common.setting.EzyMQRpcSettings;
import lombok.Getter;

import java.util.*;

import static com.tvd12.ezymq.activemq.util.EzyActiveHandlerAnnotations.getCommand;
import static com.tvd12.properties.file.util.PropertiesUtil.getFirstPropertyKeys;
import static com.tvd12.properties.file.util.PropertiesUtil.getPropertiesByPrefix;

@Getter
@SuppressWarnings("rawtypes")
public class EzyActiveSettings extends EzyMQRpcSettings {

    protected final Map<String, EzyActiveTopicSetting> topicSettings;
    protected final Map<String, EzyActiveRpcProducerSetting> rpcProducerSettings;
    protected final Map<String, EzyActiveRpcConsumerSetting> rpcConsumerSettings;

    public static final String KEY_URI = "activemq.uri";
    public static final String KEY_USERNAME = "activemq.username";
    public static final String KEY_PASSWORD = "activemq.password";
    public static final String KEY_CAPACITY = "capacity";
    public static final String KEY_CONSUMER_THREAD_POOL_SIZE = "consumer_thread_pool_size";
    public static final String KEY_CONSUMERS = "activemq.consumers";
    public static final String KEY_DEFAULT_TIMEOUT = "default_timeout";
    public static final String KEY_MAX_THREAD_POOL_SIZE = "activemq.max_thread_pool_size";
    public static final String KEY_PRODUCERS = "activemq.producers";
    public static final String KEY_REPLY_QUEUE_NAME = "reply_queue_name";
    public static final String KEY_REQUEST_QUEUE_NAME = "request_queue_name";
    public static final String KEY_THREAD_POOL_SIZE = "thread_pool_size";
    public static final String KEY_TOPIC = "topic";
    public static final String KEY_TOPICS = "activemq.topics";

    public EzyActiveSettings(
        Properties properties,
        Map<String, Class> requestTypes,
        Map<String, EzyActiveTopicSetting> topicSettings,
        Map<String, EzyActiveRpcProducerSetting> rpcProducerSettings,
        Map<String, EzyActiveRpcConsumerSetting> rpcConsumerSettings
    ) {
        super(properties, requestTypes);
        this.topicSettings = Collections.unmodifiableMap(topicSettings);
        this.rpcProducerSettings = Collections.unmodifiableMap(rpcProducerSettings);
        this.rpcConsumerSettings = Collections.unmodifiableMap(rpcConsumerSettings);
    }

    public static Builder builder() {
        return new Builder();
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
            buildTopicSettings();
            buildProducerSettings();
            buildConsumerSettings();
            return new EzyActiveSettings(
                properties,
                requestTypeByCommand,
                topicSettings,
                rpcProducerSettings,
                rpcConsumerSettings
            );
        }

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
                EzyActiveTopicSetting topicSetting = topicSettingBuilders
                    .computeIfAbsent(name, k ->
                        EzyActiveTopicSetting.builder()
                    )
                    .topicName(topicProperties.getProperty(KEY_TOPIC, name))
                    .consumerThreadPoolSize(
                        Integer.parseInt(
                            topicProperties.getOrDefault(
                                KEY_CONSUMER_THREAD_POOL_SIZE,
                                0
                            ).toString()
                        )
                    )
                    .build();
                topicSettings.put(name, topicSetting);
            }
        }

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
                EzyActiveRpcProducerSetting producerSetting = rpcProducerSettingBuilders
                    .computeIfAbsent(name, k ->
                        EzyActiveRpcProducerSetting.builder()
                    )
                    .requestQueueName(
                        producerProperties.getProperty(
                            KEY_REQUEST_QUEUE_NAME,
                            name + "-request"
                        )
                    )
                    .replyQueueName(
                        producerProperties.getProperty(
                            KEY_REPLY_QUEUE_NAME,
                            name + "-reply"
                        )
                    )
                    .threadPoolSize(
                        Integer.parseInt(
                            producerProperties.getOrDefault(
                                KEY_THREAD_POOL_SIZE,
                                0
                            ).toString()
                        )
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
                EzyActiveRpcConsumerSetting consumerSetting = rpcConsumerSettingBuilders
                    .computeIfAbsent(name, k ->
                        EzyActiveRpcConsumerSetting.builder()
                    )
                    .requestQueueName(
                        consumerProperties.getProperty(
                            KEY_REQUEST_QUEUE_NAME,
                            name + "-request"
                        )
                    )
                    .replyQueueName(
                        consumerProperties.getProperty(
                            KEY_REPLY_QUEUE_NAME,
                            name + "-reply"
                        )
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
