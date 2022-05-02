package com.tvd12.ezymq.kafka.codec;

import com.tvd12.ezyfox.binding.EzyMarshaller;
import com.tvd12.ezyfox.binding.EzyUnmarshaller;
import com.tvd12.ezyfox.codec.EzyMessageDeserializer;
import com.tvd12.ezyfox.codec.EzyMessageSerializer;
import lombok.Setter;

import java.util.Map;

@Setter
@SuppressWarnings("rawtypes")
public class EzyKafkaBytesDataCodec extends EzyKafkaAbstractDataCodec {

    protected EzyMessageSerializer messageSerializer;
    protected EzyMessageDeserializer messageDeserializer;
    protected EzyMessageDeserializer textMessageDeserializer;

    public EzyKafkaBytesDataCodec() {}

    public EzyKafkaBytesDataCodec(
        EzyMessageSerializer messageSerializer,
        EzyMessageDeserializer messageDeserializer,
        EzyMessageDeserializer textMessageDeserializer
    ) {
        this.messageSerializer = messageSerializer;
        this.messageDeserializer = messageDeserializer;
        this.textMessageDeserializer = textMessageDeserializer;
    }

    public EzyKafkaBytesDataCodec(
        EzyMarshaller marshaller,
        EzyUnmarshaller unmarshaller,
        EzyMessageSerializer messageSerializer,
        EzyMessageDeserializer messageDeserializer,
        EzyMessageDeserializer textMessageDeserializer,
        Map<String, Map<String, Class>> messageTypesByTopic
    ) {
        super(marshaller, unmarshaller, messageTypesByTopic);
        this.messageSerializer = messageSerializer;
        this.messageDeserializer = messageDeserializer;
        this.textMessageDeserializer = textMessageDeserializer;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public byte[] serialize(Object response) {
        Object data = marshallEntity(response);
        return messageSerializer.serialize(data);
    }

    @Override
    public Object deserialize(String topic, String cmd, byte[] request) {
        Object data = messageDeserializer.deserialize(request);
        return unmarshallData(topic, cmd, data);
    }

    @Override
    public Object deserializeText(String topic, String cmd, byte[] request) {
        if (textMessageDeserializer == null) {
            throw new IllegalStateException(
                "textMessageDeserializer is null, " +
                    "maybe you need add ezyfox-jackson to " +
                    "your project configuration"
            );
        }
        Object data = textMessageDeserializer.deserialize(request);
        return unmarshallData(topic, cmd, data);
    }

    public static class Builder extends EzyKafkaAbstractDataCodecBuilder<Builder> {
        protected EzyMessageSerializer messageSerializer;
        protected EzyMessageDeserializer messageDeserializer;
        protected EzyMessageDeserializer textMessageDeserializer;

        public Builder messageSerializer(EzyMessageSerializer messageSerializer) {
            this.messageSerializer = messageSerializer;
            return this;
        }

        public Builder messageDeserializer(EzyMessageDeserializer messageDeserializer) {
            this.messageDeserializer = messageDeserializer;
            return this;
        }

        public Builder textMessageDeserializer(EzyMessageDeserializer textMessageDeserializer) {
            this.textMessageDeserializer = textMessageDeserializer;
            return this;
        }

        @Override
        protected EzyKafkaAbstractDataCodec newProduct() {
            return new EzyKafkaBytesDataCodec(messageSerializer, messageDeserializer, textMessageDeserializer);
        }
    }
}
