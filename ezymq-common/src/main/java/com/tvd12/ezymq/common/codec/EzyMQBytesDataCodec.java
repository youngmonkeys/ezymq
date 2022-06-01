package com.tvd12.ezymq.common.codec;

import com.tvd12.ezyfox.binding.EzyMarshaller;
import com.tvd12.ezyfox.binding.EzyUnmarshaller;
import com.tvd12.ezyfox.codec.EzyMessageDeserializer;
import com.tvd12.ezyfox.codec.EzyMessageSerializer;
import lombok.Setter;

import java.util.Map;

@Setter
@SuppressWarnings("rawtypes")
public class EzyMQBytesDataCodec extends EzyMQAbstractDataCodec {

    protected final EzyMessageSerializer messageSerializer;
    protected final EzyMessageDeserializer messageDeserializer;

    public EzyMQBytesDataCodec(
        EzyMarshaller marshaller,
        EzyUnmarshaller unmarshaller,
        EzyMessageSerializer messageSerializer,
        EzyMessageDeserializer messageDeserializer,
        Map<String, Class> requestTypeMap,
        Map<String, Map<String, Class>> messageTypeMapByTopic
    ) {
        super(
            marshaller,
            unmarshaller,
            requestTypeMap,
            messageTypeMapByTopic
        );
        this.messageSerializer = messageSerializer;
        this.messageDeserializer = messageDeserializer;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public byte[] serialize(Object entity) {
        Object value = marshallEntity(entity);
        return messageSerializer.serialize(value);
    }

    @Override
    public Object deserialize(String cmd, byte[] bytes) {
        Object data = messageDeserializer.deserialize(bytes);
        return unmarshallData(cmd, data);
    }

    @Override
    public Object deserializeTopicMessage(
        String topic,
        String cmd,
        byte[] message
    ) {
        Object data = messageDeserializer.deserialize(message);
        return unmarshallTopicData(topic, cmd, data);
    }

    public static class Builder extends EzyMQAbstractDataCodec.Builder<Builder> {

        protected EzyMessageSerializer messageSerializer;
        protected EzyMessageDeserializer messageDeserializer;

        public Builder messageSerializer(EzyMessageSerializer messageSerializer) {
            this.messageSerializer = messageSerializer;
            return this;
        }

        public Builder messageDeserializer(EzyMessageDeserializer messageDeserializer) {
            this.messageDeserializer = messageDeserializer;
            return this;
        }

        @Override
        public EzyMQBytesDataCodec build() {
            return new EzyMQBytesDataCodec(
                marshaller,
                unmarshaller,
                messageSerializer,
                messageDeserializer,
                requestTypeByCommand,
                messageTypeMapByTopic
            );
        }
    }
}
