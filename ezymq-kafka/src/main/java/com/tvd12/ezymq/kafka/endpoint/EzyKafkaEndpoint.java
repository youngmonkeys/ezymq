package com.tvd12.ezymq.kafka.endpoint;

import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.util.EzyLoggable;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

public class EzyKafkaEndpoint extends EzyLoggable {

    protected final String topic;

    public EzyKafkaEndpoint(String topic) {
        this.topic = topic;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public abstract static class Builder<B extends Builder<B>>
        implements EzyBuilder<EzyKafkaEndpoint> {

        protected final Map<String, Object> properties;
        protected String topic;

        public Builder() {
            this.properties = new HashMap<>();
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
            this.properties.putAll(properties);
            return (B) this;
        }

        protected Producer newProducer(Serializer serializer) {
            if (serializer == null) {
                return new KafkaProducer<>(properties);
            }
            return new KafkaProducer(properties, serializer, serializer);
        }

        protected Consumer newConsumer(Deserializer deserializer) {
            if (deserializer == null) {
                return new KafkaConsumer<>(properties);
            }
            return new KafkaConsumer<>(properties, deserializer, deserializer);
        }
    }
}
