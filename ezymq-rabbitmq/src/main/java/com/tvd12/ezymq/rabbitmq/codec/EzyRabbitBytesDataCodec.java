package com.tvd12.ezymq.rabbitmq.codec;

import com.tvd12.ezyfox.binding.EzyMarshaller;
import com.tvd12.ezyfox.binding.EzyUnmarshaller;
import com.tvd12.ezyfox.codec.EzyMessageDeserializer;
import com.tvd12.ezyfox.codec.EzyMessageSerializer;
import lombok.Setter;

import java.util.Map;

@Setter
@SuppressWarnings("rawtypes")
public class EzyRabbitBytesDataCodec extends EzyRabbitAbstractDataCodec {

    protected EzyMessageSerializer messageSerializer;
    protected EzyMessageDeserializer messageDeserializer;

    public EzyRabbitBytesDataCodec(
        EzyMessageSerializer messageSerializer,
        EzyMessageDeserializer messageDeserializer
    ) {
        this.messageSerializer = messageSerializer;
        this.messageDeserializer = messageDeserializer;
    }

    public EzyRabbitBytesDataCodec(
        EzyMarshaller marshaller,
        EzyUnmarshaller unmarshaller,
        EzyMessageSerializer messageSerializer,
        EzyMessageDeserializer messageDeserializer,
        Map<String, Class> requestTypeMap
    ) {
        super(marshaller, unmarshaller, requestTypeMap);
        this.messageSerializer = messageSerializer;
        this.messageDeserializer = messageDeserializer;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Object deserialize(String cmd, byte[] request) {
        Object data = messageDeserializer.deserialize(request);
        return unmarshallData(cmd, data);
    }

    @Override
    public byte[] serialize(Object response) {
        Object data = marshallEntity(response);
        return messageSerializer.serialize(data);
    }

    public static class Builder extends EzyRabbitAbstractDataCodecBuilder<Builder> {
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
        protected EzyRabbitAbstractDataCodec newProduct() {
            return new EzyRabbitBytesDataCodec(messageSerializer, messageDeserializer);
        }
    }
}
