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

    public static final String PRODUCERS_KEY = "kafka.producers";
    public static final String CONSUMERS_KEY = "kafka.consumers";
    public static final String TOPIC_KEY = "topic";
    public static final String THREAD_POOL_SIZE_KEY = "thread_pool_size";
    public static final String MESSAGE_TYPE = "message_type";
    public static final String MESSAGE_TYPES = "message_types";

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
    public List<Class> getMessageTypeList() {
        List<Class> list = new ArrayList<>();
        for (Map<String, Class> messageByCommand : messageTypesByTopic.values()) {
            list.addAll(messageByCommand.values());
        }
        return list;
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
            this.messageTypesByTopic.putAll(messageTypesByTopic);
            return this;
        }

        public EzyKafkaProxyBuilder parent() {
            return (EzyKafkaProxyBuilder) super.parent();
        }

        @Override
        public EzyKafkaSettings build() {
            String bootstrapServers = (String) properties.get(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG
            );
            if (bootstrapServers == null) {
                bootstrapServers = "localhost:9092";
            }
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
                getPropertiesByPrefix(properties, PRODUCERS_KEY);
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
                String topic = producerProperties.getProperty(TOPIC_KEY, name);
                EzyKafkaProducerSetting producerSetting = producerSettingBuilders
                    .computeIfAbsent(name, k ->
                        EzyKafkaProducerSetting.builder()
                    )
                    .topic(topic)
                    .properties(producerProperties)
                    .build();
                producerSettings.put(name, producerSetting);
                extractAndMapMessageTypes(name, topic, producerProperties);
            }
        }

        private void buildConsumerSettings(String globalBootstrapServers) {
            Properties consumersProperties =
                getPropertiesByPrefix(properties, CONSUMERS_KEY);
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
                    ConsumerConfig.GROUP_ID_CONFIG,
                    name
                );
                String topic = consumerProperties.getProperty(TOPIC_KEY, name);
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
                                THREAD_POOL_SIZE_KEY,
                                0
                            ).toString()
                        )
                    )
                    .build();
                consumerSettings.put(name, consumerSetting);
                extractAndMapMessageTypes(name, topic, consumerProperties);
            }
        }

        private void extractAndMapMessageTypes(
            String endpointName,
            String topic,
            Properties settingProperties
        ) {
            String messageType = settingProperties.getProperty(MESSAGE_TYPE);
            if (messageType != null) {
                mapMessageType(endpointName, EzyClasses.getClass(messageType));
            }
            Properties messageTypes = getPropertiesByPrefix(
                settingProperties,
                MESSAGE_TYPES
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
