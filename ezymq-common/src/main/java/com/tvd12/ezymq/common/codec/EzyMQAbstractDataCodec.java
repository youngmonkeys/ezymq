package com.tvd12.ezymq.common.codec;

import com.tvd12.ezyfox.binding.EzyMarshaller;
import com.tvd12.ezyfox.binding.EzyUnmarshaller;
import com.tvd12.ezyfox.builder.EzyBuilder;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class EzyMQAbstractDataCodec implements EzyMQDataCodec {

    protected final EzyMarshaller marshaller;
    protected final EzyUnmarshaller unmarshaller;
    protected final Map<String, Class> requestTypeByCommand;
    protected final Map<String, Map<String, Class>> messageTypeMapByTopic;

    public EzyMQAbstractDataCodec(
        EzyMarshaller marshaller,
        EzyUnmarshaller unmarshaller,
        Map<String, Class> requestTypeMap,
        Map<String, Map<String, Class>> messageTypeMapByTopic
    ) {
        this.marshaller = marshaller;
        this.unmarshaller = unmarshaller;
        this.requestTypeByCommand = requestTypeMap;
        this.messageTypeMapByTopic = messageTypeMapByTopic;
    }

    protected Object marshallEntity(Object entity) {
        return marshaller.marshal(entity);
    }

    protected Object unmarshallData(String cmd, Object value) {
        Class requestType = requestTypeByCommand.get(cmd);
        if (requestType == null) {
            throw new IllegalArgumentException(
                "has no request type with command: " + cmd
            );
        }
        return unmarshaller.unmarshal(value, requestType);
    }

    protected Object unmarshallTopicData(
        String topic,
        String cmd,
        Object value
    ) {
        Class requestType = messageTypeMapByTopic.getOrDefault(
            topic,
            Collections.emptyMap()
        ).get(cmd);
        if (requestType == null) {
            throw new IllegalArgumentException(
                "has no message type with topic: " + topic +
                    " and command: " + cmd
            );
        }
        return unmarshaller.unmarshal(value, requestType);
    }

    public abstract static class Builder<B extends Builder>
        implements EzyBuilder<EzyMQDataCodec> {

        protected EzyMarshaller marshaller;
        protected EzyUnmarshaller unmarshaller;
        protected final Map<String, Class> requestTypeByCommand =
            new HashMap<>();
        protected final Map<String, Map<String, Class>> messageTypeMapByTopic =
            new HashMap<>();

        public B marshaller(EzyMarshaller marshaller) {
            this.marshaller = marshaller;
            return (B) this;
        }

        public B unmarshaller(EzyUnmarshaller unmarshaller) {
            this.unmarshaller = unmarshaller;
            return (B) this;
        }

        public B mapRequestType(String cmd, Class requestType) {
            this.requestTypeByCommand.put(cmd, requestType);
            return (B) this;
        }

        public B mapRequestTypes(Map<String, Class> requestTypes) {
            this.requestTypeByCommand.putAll(requestTypes);
            return (B) this;
        }

        public B mapTopicMessageType(
            String topic,
            String cmd,
            Class<?> messageType
        ) {
            this.messageTypeMapByTopic.computeIfAbsent(
                topic,
                k -> new HashMap<>()
            ).put(cmd, messageType);
            return (B) this;
        }

        public B mapTopicMessageTypes(
            String topic,
            Map<String, Class> messageTypes
        ) {
            this.messageTypeMapByTopic.computeIfAbsent(
                topic,
                k -> new HashMap<>()
            ).putAll(messageTypes);
            return (B) this;
        }

        public B mapTopicMessageTypes(
            Map<String, Map<String, Class>> messageTypeMapByTopic
        ) {
            for (String topic : messageTypeMapByTopic.keySet()) {
                mapTopicMessageTypes(topic, messageTypeMapByTopic.get(topic));
            }
            return (B) this;
        }
    }
}
