package com.tvd12.ezymq.kafka.setting;

import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.reflect.EzyClasses;
import com.tvd12.ezymq.kafka.EzyKafkaProxyBuilder;
import com.tvd12.ezymq.kafka.annotation.EzyKafkaHandler;
import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageHandler;
import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageInterceptor;
import com.tvd12.properties.file.util.PropertiesUtil;
import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.*;

@Getter
@SuppressWarnings("rawtypes")
public class EzyKafkaSettings {

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
        Map<String, Map<String, Class>> messageTypesByTopic,
        Map<String, EzyKafkaProducerSetting> consumerSettings,
        Map<String, EzyKafkaConsumerSetting> handlerSettings
    ) {
        this.producerSettings = Collections.unmodifiableMap(consumerSettings);
        this.consumerSettings = Collections.unmodifiableMap(handlerSettings);
        this.messageTypesByTopic = Collections.unmodifiableMap(messageTypesByTopic);
    }

    public static Builder builder() {
        return new Builder();
    }

    public List<Class> getMessageTypeList() {
        List<Class> list = new ArrayList<>();
        for (Map<String, Class> messageByCommand : messageTypesByTopic.values()) {
            list.addAll(messageByCommand.values());
        }
        return list;
    }

    public static class Builder implements EzyBuilder<EzyKafkaSettings> {

        protected EzyKafkaProxyBuilder parent;
        protected Map<String, Object> properties;
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
            this.parent = parent;
            this.properties = new HashMap<>();
            this.producerSettings = new HashMap<>();
            this.consumerSettings = new HashMap<>();
            this.messageTypesByTopic = new HashMap<>();
            this.consumerInterceptors = new ArrayList<>();
            this.producerSettingBuilders = new HashMap<>();
            this.consumerSettingBuilders = new HashMap<>();
            this.consumerMessageHandlers = new HashMap<>();
        }

        public Builder property(String key, Object value) {
            this.properties.put(key, value);
            return this;
        }

        public Builder properties(Map<String, Object> properties) {
            this.properties.putAll(properties);
            return this;
        }

        public Builder consumerInterceptor(EzyKafkaMessageInterceptor consumerInterceptor) {
            this.consumerInterceptors.add(consumerInterceptor);
            return this;
        }

        public Builder consumerInterceptors(Collection<EzyKafkaMessageInterceptor> consumerInterceptors) {
            this.consumerInterceptors.addAll(consumerInterceptors);
            return this;
        }

        public Builder consumerMessageHandlers(List<EzyKafkaMessageHandler> consumerMessageHandlers) {
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

        public EzyKafkaProducerSetting.Builder producerSettingBuilder(String name) {
            return producerSettingBuilders.computeIfAbsent(
                name, k -> new EzyKafkaProducerSetting.Builder(this));
        }

        public EzyKafkaConsumerSetting.Builder consumerSettingBuilder(String name) {
            return consumerSettingBuilders.computeIfAbsent(
                name, k -> new EzyKafkaConsumerSetting.Builder(this));
        }

        public Builder addProducerSetting(String name, EzyKafkaProducerSetting setting) {
            this.producerSettings.put(name, setting);
            return this;
        }

        public Builder addConsumerSetting(String name, EzyKafkaConsumerSetting setting) {
            this.consumerSettings.put(name, setting);
            return this;
        }

        public Builder mapMessageType(String topic, Class messageType) {
            return mapMessageType(topic, "", messageType);
        }

        public Builder mapMessageType(String topic, String cmd, Class messageType) {
            this.messageTypesByTopic.computeIfAbsent(topic, k -> new HashMap<>())
                .put(cmd, messageType);
            return this;
        }

        public Builder mapMessageTypes(String topic, Map<String, Class> messageTypeByCommand) {
            this.messageTypesByTopic.computeIfAbsent(topic, k -> new HashMap<>())
                .putAll(messageTypeByCommand);
            return this;
        }

        public Builder mapMessageTypes(Map<String, Map<String, Class>> messageTypesByTopic) {
            this.messageTypesByTopic.putAll(messageTypesByTopic);
            return this;
        }


        public EzyKafkaProxyBuilder parent() {
            return parent;
        }

        @SuppressWarnings({"unchecked", "MethodLength"})
        @Override
        public EzyKafkaSettings build() {
            String bootstrapServers =
                (String) properties.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
            if (bootstrapServers == null) {
                bootstrapServers = "localhost:9092";
            }
            Properties producersProperties =
                PropertiesUtil.getPropertiesByPrefix(properties, PRODUCERS_KEY);
            Set<String> producerNames = new HashSet<>();
            producerNames.addAll(producerSettingBuilders.keySet());
            producerNames.addAll(PropertiesUtil.getFirstPropertyKeys(producersProperties));
            for (String name : producerNames) {
                Properties producerProperties =
                    PropertiesUtil.getPropertiesByPrefix(producersProperties, name);
                if (!producerProperties.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
                    producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                }
                if (!producerProperties.containsKey(ProducerConfig.CLIENT_ID_CONFIG)) {
                    producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, name);
                }
                EzyKafkaProducerSetting.Builder builder = producerSettingBuilders.get(name);
                String topic = producerProperties.getProperty(TOPIC_KEY);
                if (topic == null) {
                    topic = name;
                }
                if (builder == null) {
                    builder = EzyKafkaProducerSetting.builder()
                        .topic(topic);
                }
                builder.properties((Map) producerProperties);
                producerSettings.put(name, builder.build());
                String messageType = producersProperties.getProperty(MESSAGE_TYPE);
                if (messageType != null) {
                    mapMessageType(name, EzyClasses.getClass(messageType));
                }
                Properties messageTypes = PropertiesUtil.getPropertiesByPrefix(producersProperties, MESSAGE_TYPES);
                for (Object cmd : messageTypes.keySet()) {
                    mapMessageType(topic, cmd.toString(), EzyClasses.getClass(messageTypes.get(cmd).toString()));
                }
            }
            Properties consumersProperties =
                PropertiesUtil.getPropertiesByPrefix(properties, CONSUMERS_KEY);
            Set<String> consumerNames = new HashSet<>();
            consumerNames.addAll(consumerSettingBuilders.keySet());
            consumerNames.addAll(PropertiesUtil.getFirstPropertyKeys(consumersProperties));
            for (String name : consumerNames) {
                Properties consumerProperties =
                    PropertiesUtil.getPropertiesByPrefix(consumersProperties, name);
                if (!consumerProperties.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
                    consumerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                }
                if (!consumerProperties.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
                    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, name);
                }
                String topic = consumerProperties.getProperty(TOPIC_KEY);
                if (topic == null) {
                    topic = name;
                }
                EzyKafkaConsumerSetting.Builder builder = consumerSettingBuilders.get(name);
                if (builder == null) {
                    builder = EzyKafkaConsumerSetting.builder()
                        .topic(topic);
                    if (consumerProperties.containsKey(THREAD_POOL_SIZE_KEY)) {
                        builder.threadPoolSize((int) consumerProperties.get(THREAD_POOL_SIZE_KEY));
                    }
                }
                builder.properties((Map) consumerProperties);
                builder.messageInterceptors(consumerInterceptors);
                builder.addMessageHandlers(consumerMessageHandlers.get(topic));
                consumerSettings.put(name, builder.build());
                String messageType = consumerProperties.getProperty(MESSAGE_TYPE);
                if (messageType != null) {
                    mapMessageType(name, EzyClasses.getClass(messageType));
                }
                Properties messageTypes = PropertiesUtil.getPropertiesByPrefix(consumerProperties, MESSAGE_TYPES);
                for (Object cmd : messageTypes.keySet()) {
                    mapMessageType(topic, cmd.toString(), EzyClasses.getClass(messageTypes.get(cmd).toString()));
                }
            }
            return new EzyKafkaSettings(messageTypesByTopic, producerSettings, consumerSettings);
        }
    }
}
