package com.tvd12.ezymq.kafka.setting;

import com.tvd12.ezyfox.builder.EzyBuilder;
import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.HashMap;
import java.util.Map;

@Getter
public class EzyKafkaEndpointSetting {

    protected final String topic;
    protected final Map<String, Object> properties;

    public EzyKafkaEndpointSetting(
        String topic,
        Map<String, Object> properties
    ) {
        this.topic = topic;
        this.properties = properties;
    }

    public Object getProperty(String key) {
        return properties.get(key);
    }

    public Map<String, Object> getProperties() {
        return new HashMap<>(properties);
    }

    public boolean containsProperty(String key) {
        return properties.containsKey(key);
    }

    @SuppressWarnings("unchecked")
    public abstract static class Builder<B extends Builder<B>>
        implements EzyBuilder<EzyKafkaEndpointSetting> {

        protected final Map<String, Object> properties;
        protected String topic;

        public Builder() {
            this.properties = new HashMap<>();
            this.properties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "com.tvd12.ezymq.kafka.serialization.EzyDefaultSerializer"
            );
            this.properties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "com.tvd12.ezymq.kafka.serialization.EzyDefaultSerializer"
            );
            this.properties.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "com.tvd12.ezymq.kafka.serialization.EzyDefaultDeserializer"
            );
            this.properties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "com.tvd12.ezymq.kafka.serialization.EzyDefaultDeserializer"
            );
        }

        public B topic(String topic) {
            this.topic = topic;
            return (B) this;
        }

        public B property(String key, Object value) {
            this.properties.put(key, value);
            return (B) this;
        }

        public B properties(Map<String, Object> properties) {
            if (properties != null) {
                this.properties.putAll(properties);
            }
            return (B) this;
        }
    }
}
