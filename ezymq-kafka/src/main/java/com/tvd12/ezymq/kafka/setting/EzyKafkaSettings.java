package com.tvd12.ezymq.kafka.setting;

import com.tvd12.ezyfox.reflect.EzyClasses;
import com.tvd12.ezymq.common.setting.EzyMQSettings;
import com.tvd12.ezymq.kafka.EzyKafkaProxyBuilder;
import com.tvd12.ezymq.kafka.annotation.EzyKafkaHandler;
import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageHandler;
import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageInterceptor;
import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.*;

import static com.tvd12.properties.file.util.PropertiesUtil.getFirstPropertyKeys;
import static com.tvd12.properties.file.util.PropertiesUtil.getPropertiesByPrefix;
import static java.util.Collections.emptyMap;

@Getter
@SuppressWarnings("rawtypes")
public class EzyKafkaSettings extends EzyMQSettings {

    protected final Map<String, Map<String, Class>> messageTypesByTopic;
    protected final Map<String, EzyKafkaProducerSetting> producerSettings;
    protected final Map<String, EzyKafkaConsumerSetting> consumerSettings;

    public static final String KEY_CONSUMERS = "kafka.consumers";
    public static final String KEY_MESSAGE_TYPE = "message_type";
    public static final String KEY_MESSAGE_TYPES = "message_types";
    public static final String KEY_PRODUCERS = "kafka.producers";
    public static final String KEY_THREAD_POOL_SIZE = "thread_pool_size";
    public static final String KEY_TOPIC = "topic";

    public EzyKafkaSettings(
        Properties properties,
        Map<String, Map<String, Class>> messageTypesByTopic,
        Map<String, EzyKafkaProducerSetting> consumerSettings,
        Map<String, EzyKafkaConsumerSetting> handlerSettings
    ) {
        super(properties);
        this.producerSettings = Collections.unmodifiableMap(consumerSettings);
        this.consumerSettings = Collections.unmodifiableMap(handlerSettings);
        this.messageTypesByTopic = Collections.unmodifiableMap(messageTypesByTopic);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Set<Class> getMessageTypes() {
        Set<Class> set = new HashSet<>();
        for (Map<String, Class> messageByCommand : messageTypesByTopic.values()) {
            set.addAll(messageByCommand.values());
        }
        return set;
    }

    public static class Builder
        extends EzyMQSettings.Builder<EzyKafkaSettings, Builder> {

        protected Map<String, Map<String, Class>> messageTypesByTopic;
        protected Map<String, EzyKafkaProducerSetting> producerSettings;
        protected Map<String, EzyKafkaConsumerSetting> consumerSettings;
        protected List<EzyKafkaMessageInterceptor> consumerInterceptors;
        protected Map<String, EzyKafkaProducerSetting.Builder> producerSettingBuilders;
        protected Map<String, EzyKafkaConsumerSetting.Builder> consumerSettingBuilders;
        protected Map<String, Map<String, EzyKafkaMessageHandler>> consumerMessageHandlers;

        public Builder() {
            this(null);
        }

        public Builder(EzyKafkaProxyBuilder parent) {
            super(parent);
            this.producerSettings = new HashMap<>();
            this.consumerSettings = new HashMap<>();
            this.messageTypesByTopic = new HashMap<>();
            this.consumerInterceptors = new ArrayList<>();
            this.producerSettingBuilders = new HashMap<>();
            this.consumerSettingBuilders = new HashMap<>();
            this.consumerMessageHandlers = new HashMap<>();
        }

        public Builder addConsumerInterceptor(
            EzyKafkaMessageInterceptor consumerInterceptor
        ) {
            this.consumerInterceptors.add(consumerInterceptor);
            return this;
        }

        public Builder addConsumerInterceptors(
            Collection<EzyKafkaMessageInterceptor> consumerInterceptors
        ) {
            this.consumerInterceptors.addAll(consumerInterceptors);
            return this;
        }

        public Builder addConsumerMessageHandlers(
            List<EzyKafkaMessageHandler> consumerMessageHandlers
        ) {
            for (EzyKafkaMessageHandler handler : consumerMessageHandlers) {
                EzyKafkaHandler anno = handler.getClass().getAnnotation(EzyKafkaHandler.class);
                String topic = anno.topic();
                String command = anno.command();
                this.consumerMessageHandlers
                    .computeIfAbsent(topic, k -> new HashMap<>())
                    .put(command, handler);
                mapMessageType(topic, command, handler.getMessageType());
            }
            return this;
        }

        public EzyKafkaProducerSetting.Builder producerSettingBuilder(
            String name
        ) {
            return producerSettingBuilders.computeIfAbsent(
                name, k -> new EzyKafkaProducerSetting.Builder(this));
        }

        public EzyKafkaConsumerSetting.Builder consumerSettingBuilder(
            String name
        ) {
            return consumerSettingBuilders.computeIfAbsent(
                name, k -> new EzyKafkaConsumerSetting.Builder(this));
        }

        public Builder addProducerSetting(
            String name,
            EzyKafkaProducerSetting setting
        ) {
            this.producerSettings.put(name, setting);
            return this;
        }

        public Builder addConsumerSetting(
            String name,
            EzyKafkaConsumerSetting setting
        ) {
            this.consumerSettings.put(name, setting);
            return this;
        }

        public Builder mapMessageType(
            String topic,
            Class messageType
        ) {
            return mapMessageType(topic, "", messageType);
        }

        public Builder mapMessageType(
            String topic,
            String cmd,
            Class messageType
        ) {
            this.messageTypesByTopic.computeIfAbsent(topic, k -> new HashMap<>())
                .put(cmd, messageType);
            return this;
        }

        public Builder mapMessageTypes(
            String topic,
            Map<String, Class> messageTypeByCommand
        ) {
            this.messageTypesByTopic.computeIfAbsent(topic, k -> new HashMap<>())
                .putAll(messageTypeByCommand);
            return this;
        }

        public Builder mapMessageTypes(
            Map<String, Map<String, Class>> messageTypesByTopic
        ) {
            for (String topic : messageTypesByTopic.keySet()) {
                mapMessageTypes(topic, messageTypesByTopic.get(topic));
            }
            return this;
        }

        public EzyKafkaProxyBuilder parent() {
            return (EzyKafkaProxyBuilder) super.parent();
        }

        @Override
        public EzyKafkaSettings build() {
            String bootstrapServers = properties.getProperty(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092"
            );
            buildProducerSettings(bootstrapServers);
            buildConsumerSettings(bootstrapServers);

            return new EzyKafkaSettings(
                properties,
                messageTypesByTopic,
                producerSettings,
                consumerSettings
            );
        }

        private void buildProducerSettings(String globalBootstrapServers) {
            Properties producersProperties =
                getPropertiesByPrefix(properties, KEY_PRODUCERS);
            Set<String> producerNames = new HashSet<>();
            producerNames.addAll(producerSettingBuilders.keySet());
            producerNames.addAll(getFirstPropertyKeys(producersProperties));
            for (String name : producerNames) {
                Properties producerProperties = getPropertiesByPrefix(
                    producersProperties,
                    name
                );
                producerProperties.putIfAbsent(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    globalBootstrapServers
                );
                producerProperties.putIfAbsent(
                    ProducerConfig.CLIENT_ID_CONFIG,
                    name
                );
                String topic = producerProperties.getProperty(KEY_TOPIC, name);
                EzyKafkaProducerSetting producerSetting = producerSettingBuilders
                    .computeIfAbsent(name, k ->
                        EzyKafkaProducerSetting.builder()
                    )
                    .topic(topic)
                    .properties(producerProperties)
                    .build();
                producerSettings.put(name, producerSetting);
                extractAndMapMessageTypes(topic, producerProperties);
            }
        }

        private void buildConsumerSettings(String globalBootstrapServers) {
            Properties consumersProperties =
                getPropertiesByPrefix(properties, KEY_CONSUMERS);
            Set<String> consumerNames = new HashSet<>();
            consumerNames.addAll(consumerSettingBuilders.keySet());
            consumerNames.addAll(getFirstPropertyKeys(consumersProperties));
            for (String name : consumerNames) {
                Properties consumerProperties = getPropertiesByPrefix(
                    consumersProperties,
                    name
                );
                consumerProperties.putIfAbsent(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    globalBootstrapServers
                );
                consumerProperties.putIfAbsent(
                    ProducerConfig.CLIENT_ID_CONFIG,
                    name
                );
                consumerProperties.computeIfAbsent(
                    ConsumerConfig.GROUP_ID_CONFIG,
                    k -> UUID.randomUUID().toString()
                );
                String topic = consumerProperties.getProperty(KEY_TOPIC, name);
                EzyKafkaConsumerSetting consumerSetting = consumerSettingBuilders
                    .computeIfAbsent(name, k ->
                        EzyKafkaConsumerSetting.builder()
                    )
                    .topic(topic)
                    .properties(consumerProperties)
                    .addMessageInterceptors(consumerInterceptors)
                    .addMessageHandlers(
                        consumerMessageHandlers.getOrDefault(topic, emptyMap())
                    )
                    .threadPoolSize(
                        Integer.parseInt(
                            consumerProperties.getOrDefault(
                                KEY_THREAD_POOL_SIZE,
                                0
                            ).toString()
                        )
                    )
                    .build();
                consumerSettings.put(name, consumerSetting);
                extractAndMapMessageTypes(topic, consumerProperties);
            }
        }

        private void extractAndMapMessageTypes(
            String topic,
            Properties settingProperties
        ) {
            String messageType = settingProperties.getProperty(KEY_MESSAGE_TYPE);
            if (messageType != null) {
                mapMessageType(topic, EzyClasses.getClass(messageType));
            }
            Properties messageTypes = getPropertiesByPrefix(
                settingProperties,
                KEY_MESSAGE_TYPES
            );
            for (String cmd : messageTypes.stringPropertyNames()) {
                mapMessageType(
                    topic,
                    cmd,
                    EzyClasses.getClass(messageTypes.getProperty(cmd))
                );
            }
        }
    }
}
