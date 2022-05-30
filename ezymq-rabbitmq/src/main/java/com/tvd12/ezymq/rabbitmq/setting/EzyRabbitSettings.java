package com.tvd12.ezymq.rabbitmq.setting;

import com.tvd12.ezymq.common.setting.EzyMQRpcSettings;
import com.tvd12.ezymq.rabbitmq.EzyRabbitMQProxyBuilder;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitRequestHandler;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitRequestInterceptor;
import lombok.Getter;

import java.util.*;

import static com.tvd12.ezymq.rabbitmq.util.EzyRabbitHandlerAnnotations.getCommand;
import static com.tvd12.properties.file.util.PropertiesUtil.getFirstPropertyKeys;
import static com.tvd12.properties.file.util.PropertiesUtil.getPropertiesByPrefix;

@Getter
@SuppressWarnings("rawtypes")
public class EzyRabbitSettings extends EzyMQRpcSettings {

    protected final Map<String, Map<String, Object>> queueArguments;
    protected final Map<String, EzyRabbitTopicSetting> topicSettings;
    protected final Map<String, EzyRabbitRpcProducerSetting> rpcProducerSettings;
    protected final Map<String, EzyRabbitRpcConsumerSetting> rpcConsumerSettings;

    public static final String KEY_URI = "rabbitmq.uri";
    public static final String KEY_HOST = "rabbitmq.host";
    public static final String KEY_PORT = "rabbitmq.port";
    public static final String KEY_USERNAME = "rabbitmq.username";
    public static final String KEY_PASSWORD = "rabbitmq.password";
    public static final String KEY_VHOST = "rabbitmq.vhost";
    public static final String KEY_MAX_CONNECTION_ATTEMPTS = "rabbitmq.requested_heart_beat";
    public static final String KEY_REQUESTED_HEART_BEAT = "rabbitmq.max_connection_attempts";
    public static final String KEY_SHARED_THREAD_POOL_SIZE = "rabbitmq.shared_thread_pool_size";
    public static final String KEY_CAPACITY = "capacity";
    public static final String KEY_CONSUMER = "consumer";
    public static final String KEY_CONSUMER_THREAD_POOL_SIZE = "consumer_thread_pool_size";
    public static final String KEY_CONSUMERS = "rabbitmq.consumers";
    public static final String KEY_DEFAULT_TIMEOUT = "default_timeout";
    public static final String KEY_ENABLE = "enable";
    public static final String KEY_EXCHANGE = "exchange";
    public static final String KEY_MAX_THREAD_POOL_SIZE = "rabbitmq.max_thread_pool_size";
    public static final String KEY_PREFETCH_COUNT = "prefetch_count";
    public static final String KEY_PRODUCERS = "rabbitmq.producers";
    public static final String KEY_PRODUCER = "producer";
    public static final String KEY_QUEUE_NAME = "queue_name";
    public static final String KEY_REPLY_QUEUE_NAME = "reply_queue_name";
    public static final String KEY_REPLY_ROUTING_KEY = "reply_queue_name";
    public static final String KEY_REQUEST_QUEUE_NAME = "request_queue_name";
    public static final String KEY_REQUEST_ROUTING_KEY = "request_routing_key";
    public static final String KEY_ROUTING_KEY = "routing_key";
    public static final String KEY_THREAD_POOL_SIZE = "thread_pool_size";
    public static final String KEY_TOPICS = "rabbitmq.topics";

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
            buildTopicSettings();
            buildProducerSettings();
            buildConsumerSettings();
            return new EzyRabbitSettings(
                properties,
                requestTypeByCommand,
                queueArguments,
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
                EzyRabbitTopicSetting.Builder builder = topicSettingBuilders
                    .computeIfAbsent(name, k ->
                        EzyRabbitTopicSetting.builder()
                    )
                    .exchange(
                        topicProperties.getProperty(
                            KEY_EXCHANGE,
                            "topic-" + name + "-exchange"
                        )
                    )
                    .prefetchCount(
                        Integer.parseInt(
                            topicProperties.getOrDefault(
                                KEY_PREFETCH_COUNT,
                                0
                            ).toString()
                        )
                    );
                if (topicProperties.containsKey(KEY_PRODUCER)) {
                    Properties producerProperties = getPropertiesByPrefix(
                        topicProperties,
                        KEY_PRODUCER
                    );
                    builder
                        .producerEnable(true)
                        .producerRoutingKey(
                            producerProperties.getProperty(
                                KEY_ROUTING_KEY,
                                "topic-" + name + "-routing-key"
                            )
                        );
                }
                if (topicProperties.containsKey(KEY_CONSUMER)) {
                    Properties consumerProperties = getPropertiesByPrefix(
                        topicProperties,
                        KEY_CONSUMER
                    );
                    builder
                        .consumerEnable(true)
                        .consumerQueueName(
                            consumerProperties.getProperty(
                                KEY_QUEUE_NAME,
                                "topic-" + name + "-queue"
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
                EzyRabbitRpcProducerSetting producerSetting = rpcProducerSettingBuilders
                    .computeIfAbsent(name, k ->
                        EzyRabbitRpcProducerSetting.builder()
                    )
                    .exchange(
                        producerProperties.getProperty(
                            KEY_EXCHANGE,
                            name + "-exchange"
                        )
                    )
                    .prefetchCount(
                        Integer.parseInt(
                            producerProperties.getOrDefault(
                                KEY_PREFETCH_COUNT,
                                0
                            ).toString()
                        )
                    )
                    .requestQueueName(
                        producerProperties.getProperty(
                            KEY_REQUEST_QUEUE_NAME,
                            name + "-request-queue"
                        )
                    )
                    .requestRoutingKey(
                        producerProperties.getProperty(
                            KEY_REQUEST_ROUTING_KEY,
                            name + "-request-routing-key"
                        )
                    )
                    .replyQueueName(
                        producerProperties.getProperty(
                            KEY_REPLY_QUEUE_NAME,
                            name + "-reply-queue"
                        )
                    )
                    .replyRoutingKey(
                        producerProperties.getProperty(
                            KEY_REPLY_ROUTING_KEY,
                            name + "-reply-routing-key"
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
                EzyRabbitRpcConsumerSetting consumerSetting = rpcConsumerSettingBuilders
                    .computeIfAbsent(name, k ->
                        EzyRabbitRpcConsumerSetting.builder()
                    )
                    .exchange(
                        consumerProperties.getProperty(
                            KEY_EXCHANGE,
                            name + "-exchange"
                        )
                    )
                    .prefetchCount(
                        Integer.parseInt(
                            consumerProperties.getOrDefault(
                                KEY_PREFETCH_COUNT,
                                0
                            ).toString()
                        )
                    )
                    .requestQueueName(
                        consumerProperties.getProperty(
                            KEY_REQUEST_QUEUE_NAME,
                            name + "-request-queue"
                        )
                    )
                    .replyRoutingKey(
                        consumerProperties.getProperty(
                            KEY_REPLY_ROUTING_KEY,
                            name + "-reply-routing-key"
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
