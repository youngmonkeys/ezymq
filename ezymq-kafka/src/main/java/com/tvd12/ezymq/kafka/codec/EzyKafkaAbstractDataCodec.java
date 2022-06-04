package com.tvd12.ezymq.kafka.codec;

import com.tvd12.ezyfox.binding.EzyMarshaller;
import com.tvd12.ezyfox.binding.EzyUnmarshaller;
import com.tvd12.ezyfox.builder.EzyBuilder;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class EzyKafkaAbstractDataCodec implements EzyKafkaDataCodec {

    protected EzyMarshaller marshaller;
    protected EzyUnmarshaller unmarshaller;
    protected Map<String, Map<String, Class>> messageTypesByTopic;

    public EzyKafkaAbstractDataCodec(
        EzyMarshaller marshaller,
        EzyUnmarshaller unmarshaller,
        Map<String, Map<String, Class>> messageTypesByTopic
    ) {
        this.marshaller = marshaller;
        this.unmarshaller = unmarshaller;
        this.messageTypesByTopic = messageTypesByTopic;
    }

    protected Object marshallEntity(Object entity) {
        return marshaller.marshal(entity);
    }

    protected Object unmarshallData(String topic, String cmd, Object value) {
        Class messageType = messageTypesByTopic
            .getOrDefault(topic, Collections.emptyMap())
            .get(cmd);
        if (messageType == null) {
            throw new IllegalArgumentException(
                "has no message type mapped to topic: " +
                    topic + " and command: " + cmd
            );
        }
        return unmarshaller.unmarshal(value, messageType);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public abstract static class Builder<B extends Builder<B>>
        implements EzyBuilder<EzyKafkaDataCodec> {

        protected EzyMarshaller marshaller;
        protected EzyUnmarshaller unmarshaller;
        protected Map<String, Map<String, Class>> messageTypesByTopic = new HashMap<>();

        public B marshaller(EzyMarshaller marshaller) {
            this.marshaller = marshaller;
            return (B) this;
        }

        public B unmarshaller(EzyUnmarshaller unmarshaller) {
            this.unmarshaller = unmarshaller;
            return (B) this;
        }

        public B mapMessageType(String topic, Class messageType) {
            return mapMessageType(topic, "", messageType);
        }

        public B mapMessageType(String topic, String cmd, Class messageType) {
            this.messageTypesByTopic.computeIfAbsent(topic, k -> new HashMap<>())
                .put(cmd, messageType);
            return (B) this;
        }

        public B mapMessageTypes(String topic, Map<String, Class> messageTypeByCommand) {
            this.messageTypesByTopic.computeIfAbsent(topic, k -> new HashMap<>())
                .putAll(messageTypeByCommand);
            return (B) this;
        }

        public B mapMessageTypes(Map<String, Map<String, Class>> messageTypesByTopic) {
            for (String topic : messageTypesByTopic.keySet()) {
                mapMessageTypes(topic, messageTypesByTopic.get(topic));
            }
            return (B) this;
        }
    }
}
